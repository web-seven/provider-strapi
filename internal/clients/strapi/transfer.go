/*
Copyright 2025 The Crossplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package strapi

import (
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
)

// TransferClient pulls a project dump from Strapi's remote data-transfer
// endpoint. It authenticates with a transfer token scoped to "pull"
// (Settings > Transfer Tokens in the Strapi admin panel) rather than an
// admin session, and speaks the same JSON-over-WebSocket protocol as the
// `strapi transfer` CLI's remote-pull provider — see @strapi/data-transfer's
// src/strapi/remote/handlers/pull.ts and src/types/remote/protocol/**.
//
// Unlike the CLI, the archive this produces is a bespoke, human-inspectable
// format (gzip-compressed JSON-lines per stage, plus raw asset files) — it
// does not reproduce the CLI's own encrypted export archive and the output
// is not consumable by `strapi import`. Restoring from it is left to a
// future Restore resource.
type TransferClient struct {
	cfg TransferConfig
}

// TransferConfig configures a TransferClient.
type TransferConfig struct {
	// Endpoint is the Strapi base URL, e.g. https://strapi.example.com.
	Endpoint string
	// Token is a transfer token scoped to "pull".
	Token                 string
	InsecureSkipTLSVerify bool
}

const (
	transferPullPath     = "/admin/transfer/runner/pull"
	wsHandshakeTimeout   = 15 * time.Second
	readIdleTimeout      = 5 * time.Minute
	transferStepEntities = "entities"
	transferStepLinks    = "links"
	transferStepConfig   = "configuration"
	transferStepAssets   = "assets"
)

// ErrTransferUnauthorized is returned when Strapi rejects the transfer token,
// e.g. because it was revoked, regenerated or lacks the pull scope.
var ErrTransferUnauthorized = errors.New("transfer token rejected")

// IsTransferUnauthorized reports whether err was caused by Strapi rejecting
// the transfer token.
func IsTransferUnauthorized(err error) bool {
	return errors.Is(err, ErrTransferUnauthorized)
}

// NewTransferClient constructs a TransferClient.
func NewTransferClient(cfg TransferConfig) (*TransferClient, error) {
	if cfg.Endpoint == "" {
		return nil, errors.New("endpoint is required")
	}
	if cfg.Token == "" {
		return nil, errors.New("transfer token is required")
	}
	return &TransferClient{cfg: cfg}, nil
}

// DumpSink receives the objects a Dump produces. Create opens the object
// stored under key, e.g. "entities.jsonl.gz" or "assets/<name>"; Dump writes
// its full content and then closes it. Several objects may be open at once,
// since Strapi can interleave assets. If Dump fails, objects it opened may be
// left unclosed, and the sink is responsible for discarding them.
type DumpSink interface {
	Create(ctx context.Context, key string) (io.WriteCloser, error)
}

// DumpFile describes one object written by Dump. Size is the number of bytes
// written. Count is the number of JSON-lines records for stage objects, and
// zero for asset objects (each DumpFile is one asset).
type DumpFile struct {
	Key   string
	Size  int64
	Count int64
}

// DumpManifest is everything Dump wrote to the sink.
type DumpManifest struct {
	Entities      DumpFile
	Links         DumpFile
	Configuration DumpFile
	Assets        []DumpFile
}

// Dump pulls entities, links and configuration (and, if includeAssets is
// true, media assets) from Strapi and streams them to sink as they arrive,
// so nothing is buffered on disk.
func (t *TransferClient) Dump(ctx context.Context, sink DumpSink, includeAssets bool) (DumpManifest, error) {
	s, closeSession, err := t.startSession(ctx)
	if err != nil {
		return DumpManifest{}, err
	}
	defer closeSession()

	if err := s.action(ctx, "bootstrap"); err != nil {
		return DumpManifest{}, err
	}

	manifest, err := s.pullStages(ctx, sink)
	if err != nil {
		return manifest, err
	}

	if includeAssets {
		assets, err := s.pullAssets(ctx, sink)
		if err != nil {
			return manifest, err
		}
		manifest.Assets = assets
	}

	if err := s.action(ctx, "close"); err != nil {
		return manifest, err
	}
	if err := s.end(ctx); err != nil {
		return manifest, err
	}
	return manifest, nil
}

// startSession dials the transfer WebSocket and runs the init command,
// returning a session ready for action/step calls plus a func that tears
// down the connection. The returned teardown also cancels the background
// watcher that force-closes the socket if ctx is cancelled mid-read.
func (t *TransferClient) startSession(ctx context.Context) (*pullSession, func(), error) {
	conn, err := t.connect(ctx)
	if err != nil {
		return nil, nil, err
	}
	closeWatcher := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			conn.Close() //nolint:errcheck,gosec // best-effort unblock of a pending read on cancellation
		case <-closeWatcher:
		}
	}()
	teardown := func() {
		close(closeWatcher)
		conn.Close() //nolint:errcheck
	}

	s := &pullSession{conn: conn}
	if err := s.init(ctx); err != nil {
		teardown()
		return nil, nil, err
	}
	return s, teardown, nil
}

// pullStages requests the entities/links/configuration stages in order.
func (s *pullSession) pullStages(ctx context.Context, sink DumpSink) (DumpManifest, error) {
	var manifest DumpManifest
	for _, step := range []struct {
		name string
		dst  *DumpFile
	}{
		{transferStepEntities, &manifest.Entities},
		{transferStepLinks, &manifest.Links},
		{transferStepConfig, &manifest.Configuration},
	} {
		df, err := s.pullJSONLStep(ctx, step.name, sink)
		if err != nil {
			return manifest, err
		}
		*step.dst = df
	}
	return manifest, nil
}

func (t *TransferClient) connect(ctx context.Context) (*websocket.Conn, error) {
	wsURL, err := transferWebSocketURL(t.cfg.Endpoint)
	if err != nil {
		return nil, err
	}

	dialer := websocket.Dialer{HandshakeTimeout: wsHandshakeTimeout}
	if t.cfg.InsecureSkipTLSVerify {
		dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // explicit opt-in
	}

	conn, resp, err := dialer.DialContext(ctx, wsURL, http.Header{ //nolint:bodyclose // gorilla/websocket docs: resp.Body "does not need to be closed by the application"
		"Authorization": []string{"Bearer " + t.cfg.Token},
	})
	if err != nil {
		if resp != nil {
			if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
				return nil, errors.Wrapf(ErrTransferUnauthorized, "connect to %s: %s: %v", wsURL, resp.Status, err)
			}
			return nil, errors.Wrapf(err, "connect to %s: %s", wsURL, resp.Status)
		}
		return nil, errors.Wrapf(err, "connect to %s", wsURL)
	}
	return conn, nil
}

func transferWebSocketURL(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", errors.Wrap(err, "parse endpoint")
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	default:
		return "", errors.Errorf("unsupported endpoint scheme %q", u.Scheme)
	}
	u.Path = strings.TrimRight(u.Path, "/") + transferPullPath
	return u.String(), nil
}

// wireEnvelope is a superset of the client command, transfer action and
// transfer step message shapes used by Strapi's remote data-transfer
// protocol, plus the server's response envelope (uuid/data/error). Fields
// unused by a given message are omitted on encode via `omitempty`.
type wireEnvelope struct {
	Type       string          `json:"type,omitempty"`
	Kind       string          `json:"kind,omitempty"`
	Command    string          `json:"command,omitempty"`
	Action     string          `json:"action,omitempty"`
	Step       string          `json:"step,omitempty"`
	TransferID string          `json:"transferID,omitempty"`
	UUID       string          `json:"uuid,omitempty"`
	Params     any             `json:"params,omitempty"`
	Data       json.RawMessage `json:"data,omitempty"`
	Error      *wireError      `json:"error,omitempty"`
}

type wireError struct {
	Message string `json:"message"`
}

// stageFrame is the payload of a message the server pushes while streaming a
// step: one batch of items, or the end of the step. Strapi only pushes the
// next frame once the previous one has been acknowledged.
type stageFrame struct {
	Type  string          `json:"type"`
	ID    string          `json:"id"`
	Data  json.RawMessage `json:"data"`
	Ended bool            `json:"ended"`
	Error *wireError      `json:"error"`
}

// pullSession drives one end-to-end pull transfer over a single WebSocket
// connection. It is not safe for concurrent use — Dump talks to Strapi
// strictly sequentially, one stage at a time.
type pullSession struct {
	conn       *websocket.Conn
	transferID string
	// pending holds frames read while waiting for a specific uuid that
	// turned out to belong to something else (e.g. a stream chunk that
	// arrived before the ack for the request that triggered it). They are
	// replayed, in order, to the next reader before any new frame is read
	// off the socket, so no message is ever dropped regardless of exactly
	// how the server interleaves acks with pushed data.
	pending []wireEnvelope
}

func (s *pullSession) nextFrame(ctx context.Context) (wireEnvelope, error) {
	if len(s.pending) > 0 {
		env := s.pending[0]
		s.pending = s.pending[1:]
		return env, nil
	}
	return s.readFrame(ctx)
}

func (s *pullSession) readFrame(ctx context.Context) (wireEnvelope, error) {
	if err := ctx.Err(); err != nil {
		return wireEnvelope{}, err
	}
	if err := s.conn.SetReadDeadline(time.Now().Add(readIdleTimeout)); err != nil {
		return wireEnvelope{}, errors.Wrap(err, "set read deadline")
	}
	_, raw, err := s.conn.ReadMessage()
	if err != nil {
		return wireEnvelope{}, errors.Wrap(err, "read transfer message")
	}
	var env wireEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return wireEnvelope{}, errors.Wrap(err, "decode transfer message")
	}
	if env.Error != nil {
		return wireEnvelope{}, errors.Errorf("strapi transfer error: %s", env.Error.Message)
	}
	return env, nil
}

// dispatch sends payload with a freshly generated uuid and waits for the
// response carrying the same uuid, buffering any other frames observed
// along the way.
func (s *pullSession) dispatch(ctx context.Context, payload wireEnvelope) (wireEnvelope, error) {
	id := uuid.NewString()
	payload.UUID = id

	b, err := json.Marshal(payload)
	if err != nil {
		return wireEnvelope{}, errors.Wrap(err, "encode transfer message")
	}
	if err := s.conn.WriteMessage(websocket.TextMessage, b); err != nil {
		return wireEnvelope{}, errors.Wrap(err, "send transfer message")
	}

	// A frame matching id may already be sitting in pending from a previous
	// call. Check without disturbing the order of the rest.
	for i, env := range s.pending {
		if env.UUID == id {
			s.pending = append(s.pending[:i:i], s.pending[i+1:]...)
			return env, nil
		}
	}

	// Otherwise read fresh frames directly off the socket — never through
	// nextFrame, which would just hand back the frames we're about to stash
	// in pending below, looping forever without making progress.
	for {
		env, err := s.readFrame(ctx)
		if err != nil {
			return wireEnvelope{}, err
		}
		if env.UUID == id {
			return env, nil
		}
		s.pending = append(s.pending, env)
	}
}

// ack acknowledges a message pushed by the server, which waits for it
// before pushing the next one.
func (s *pullSession) ack(id string) error {
	b, err := json.Marshal(wireEnvelope{UUID: id})
	if err != nil {
		return errors.Wrap(err, "encode acknowledgement")
	}
	return errors.Wrap(s.conn.WriteMessage(websocket.TextMessage, b), "send acknowledgement")
}

func (s *pullSession) init(ctx context.Context) error {
	env, err := s.dispatch(ctx, wireEnvelope{
		Type:    "command",
		Command: "init",
		Params: map[string]any{
			"transfer":      "pull",
			"assetEncoding": "base64",
		},
	})
	if err != nil {
		return errors.Wrap(err, "init transfer")
	}
	var data struct {
		TransferID string `json:"transferID"`
	}
	if err := json.Unmarshal(env.Data, &data); err != nil {
		return errors.Wrap(err, "decode init response")
	}
	if data.TransferID == "" {
		return errors.New("strapi did not return a transferID")
	}
	s.transferID = data.TransferID
	return nil
}

func (s *pullSession) action(ctx context.Context, action string) error {
	_, err := s.dispatch(ctx, wireEnvelope{
		Type: "transfer", Kind: "action", TransferID: s.transferID, Action: action,
	})
	return errors.Wrapf(err, "%s transfer", action)
}

func (s *pullSession) end(ctx context.Context) error {
	_, err := s.dispatch(ctx, wireEnvelope{
		Type: "command", Command: "end", Params: map[string]any{"transferID": s.transferID},
	})
	return errors.Wrap(err, "end transfer")
}

// startStep starts a step and returns the ID the server tags the step's
// pushed frames with.
func (s *pullSession) startStep(ctx context.Context, step string) (string, error) {
	env, err := s.dispatch(ctx, wireEnvelope{
		Type: "transfer", Kind: "step", TransferID: s.transferID, Step: step, Action: "start",
	})
	if err != nil {
		return "", errors.Wrapf(err, "start %s step", step)
	}
	var data struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(env.Data, &data); err != nil {
		return "", errors.Wrapf(err, "decode start %s step response", step)
	}
	if data.ID == "" {
		return "", errors.Errorf("strapi did not return an id for the %s step", step)
	}
	return data.ID, nil
}

func (s *pullSession) endStep(ctx context.Context, step string) error {
	_, err := s.dispatch(ctx, wireEnvelope{
		Type: "transfer", Kind: "step", TransferID: s.transferID, Step: step, Action: "end",
	})
	return errors.Wrapf(err, "end %s step", step)
}

// nextStageFrame returns the next frame pushed for the step identified by
// id, acknowledging it so the server carries on streaming.
func (s *pullSession) nextStageFrame(ctx context.Context, id string) (stageFrame, error) {
	for {
		env, err := s.nextFrame(ctx)
		if err != nil {
			return stageFrame{}, err
		}
		var frame stageFrame
		if env.UUID == "" || json.Unmarshal(env.Data, &frame) != nil || frame.Type != "transfer" || frame.ID != id {
			continue
		}
		if err := s.ack(env.UUID); err != nil {
			return stageFrame{}, err
		}
		if frame.Error != nil {
			return stageFrame{}, errors.Errorf("strapi transfer error: %s", frame.Error.Message)
		}
		return frame, nil
	}
}

// batchItems decodes a pushed batch, which Strapi sends as an array of items
// or, for a single item, the item itself.
func batchItems(data json.RawMessage) []json.RawMessage {
	var items []json.RawMessage
	if err := json.Unmarshal(data, &items); err != nil {
		return []json.RawMessage{data}
	}
	return items
}

// countingWriter counts the bytes written through it.
type countingWriter struct {
	dst io.Writer
	n   int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.dst.Write(p)
	c.n += int64(n)
	return n, err
}

// pullJSONLStep requests one of the entities/links/configuration stages and
// streams each record to the sink as a line of gzip-compressed JSON.
func (s *pullSession) pullJSONLStep(ctx context.Context, step string, sink DumpSink) (DumpFile, error) {
	id, err := s.startStep(ctx, step)
	if err != nil {
		return DumpFile{}, err
	}

	key := step + ".jsonl.gz"
	w, err := sink.Create(ctx, key)
	if err != nil {
		return DumpFile{}, errors.Wrapf(err, "create %s", key)
	}
	cw := &countingWriter{dst: w}
	count, err := s.collectJSONLStep(ctx, id, cw)
	if err != nil {
		return DumpFile{}, errors.Wrapf(err, "collect %s step", step)
	}
	if err := w.Close(); err != nil {
		return DumpFile{}, errors.Wrapf(err, "write %s", key)
	}
	if err := s.endStep(ctx, step); err != nil {
		return DumpFile{}, err
	}
	return DumpFile{Key: key, Size: cw.n, Count: count}, nil
}

var newline = []byte("\n")

func (s *pullSession) collectJSONLStep(ctx context.Context, id string, w io.Writer) (int64, error) {
	gz := gzip.NewWriter(w)

	var count int64
	for {
		frame, err := s.nextStageFrame(ctx, id)
		if err != nil {
			return count, err
		}
		if frame.Ended {
			return count, gz.Close()
		}
		n, err := writeJSONLBatch(gz, frame.Data)
		count += n
		if err != nil {
			return count, err
		}
	}
}

func writeJSONLBatch(w *gzip.Writer, data json.RawMessage) (int64, error) {
	var n int64
	for _, item := range batchItems(data) {
		if _, err := w.Write(item); err != nil {
			return n, errors.Wrap(err, "write stage record")
		}
		if _, err := w.Write(newline); err != nil {
			return n, errors.Wrap(err, "write stage record")
		}
		n++
	}
	return n, nil
}

// assetMeta is the subset of Strapi's asset metadata (IFile) used to name
// the asset object.
type assetMeta struct {
	Filename string `json:"filename"`
}

// assetFlowItem is one entry of an "assets" step batch. Each asset has its
// own start/stream/end lifecycle nested inside the step's batches.
type assetFlowItem struct {
	AssetID string          `json:"assetID"`
	Action  string          `json:"action"`
	Data    json.RawMessage `json:"data"`
}

// assetObject is an asset being streamed to the sink.
type assetObject struct {
	key string
	w   io.WriteCloser
	countingWriter
}

func (s *pullSession) pullAssets(ctx context.Context, sink DumpSink) ([]DumpFile, error) {
	id, err := s.startStep(ctx, transferStepAssets)
	if err != nil {
		return nil, err
	}

	open := map[string]*assetObject{}
	var files []DumpFile
	for {
		frame, err := s.nextStageFrame(ctx, id)
		if err != nil {
			return files, err
		}
		if frame.Ended {
			return files, s.endStep(ctx, transferStepAssets)
		}
		batch, err := applyAssetBatch(ctx, sink, open, frame.Data)
		files = append(files, batch...)
		if err != nil {
			return files, err
		}
	}
}

func applyAssetBatch(ctx context.Context, sink DumpSink, open map[string]*assetObject, data json.RawMessage) ([]DumpFile, error) {
	var files []DumpFile
	for _, raw := range batchItems(data) {
		var item assetFlowItem
		if err := json.Unmarshal(raw, &item); err != nil {
			return files, errors.Wrap(err, "decode asset batch item")
		}
		df, err := applyAssetItem(ctx, sink, open, item)
		if err != nil {
			return files, err
		}
		if df != nil {
			files = append(files, *df)
		}
	}
	return files, nil
}

func applyAssetItem(ctx context.Context, sink DumpSink, open map[string]*assetObject, item assetFlowItem) (*DumpFile, error) {
	switch item.Action {
	case "start":
		return nil, startAsset(ctx, sink, open, item)
	case "stream":
		return nil, streamAssetChunk(open, item)
	case "end":
		return endAsset(open, item)
	default:
		return nil, nil
	}
}

func startAsset(ctx context.Context, sink DumpSink, open map[string]*assetObject, item assetFlowItem) error {
	var meta assetMeta
	_ = json.Unmarshal(item.Data, &meta) // best-effort; fall back to an assetID-only name
	key := "assets/" + sanitizeAssetFilename(item.AssetID, meta.Filename)
	w, err := sink.Create(ctx, key)
	if err != nil {
		return errors.Wrapf(err, "create asset %q", key)
	}
	open[item.AssetID] = &assetObject{key: key, w: w, countingWriter: countingWriter{dst: w}}
	return nil
}

func streamAssetChunk(open map[string]*assetObject, item assetFlowItem) error {
	obj, ok := open[item.AssetID]
	if !ok {
		return errors.Errorf("asset %q streamed before start", item.AssetID)
	}
	raw, err := decodeAssetChunk(item.Data)
	if err != nil {
		return errors.Wrapf(err, "decode asset %q chunk", item.AssetID)
	}
	if _, err := obj.Write(raw); err != nil {
		return errors.Wrapf(err, "write asset %q", item.AssetID)
	}
	return nil
}

func endAsset(open map[string]*assetObject, item assetFlowItem) (*DumpFile, error) {
	obj, ok := open[item.AssetID]
	if !ok {
		return nil, nil
	}
	delete(open, item.AssetID)
	if err := obj.w.Close(); err != nil {
		return nil, errors.Wrapf(err, "write asset %q", item.AssetID)
	}
	return &DumpFile{Key: obj.key, Size: obj.n}, nil
}

// decodeAssetChunk accepts both the compact base64-string wire form
// (requested via the init command's assetEncoding param) and the
// `{type:"Buffer", data:[n,n,...]}` shape Strapi 5 sends, which ignores that
// param.
func decodeAssetChunk(data json.RawMessage) ([]byte, error) {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		return base64.StdEncoding.DecodeString(s)
	}
	var legacy struct {
		Data []int `json:"data"`
	}
	if err := json.Unmarshal(data, &legacy); err != nil {
		return nil, errors.New("unrecognized asset chunk encoding")
	}
	out := make([]byte, len(legacy.Data))
	for i, v := range legacy.Data {
		out[i] = byte(v)
	}
	return out, nil
}

func sanitizeAssetFilename(assetID, filename string) string {
	base := filepath.Base(filename)
	if base == "" || base == "." || base == string(filepath.Separator) {
		base = "asset"
	}
	id := strings.NewReplacer("/", "_", "\\", "_", "..", "_").Replace(assetID)
	return fmt.Sprintf("%s_%s", id, base)
}
