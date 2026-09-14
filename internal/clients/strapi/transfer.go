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
	"net/http"
	"net/url"
	"os"
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
	transferPullPath     = "/transfer/runner/pull"
	wsHandshakeTimeout   = 15 * time.Second
	readIdleTimeout      = 5 * time.Minute
	transferStepEntities = "entities"
	transferStepLinks    = "links"
	transferStepConfig   = "configuration"
	transferStepAssets   = "assets"
)

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

// DumpFile describes one artifact written by Dump to the local staging
// directory. Count is the number of JSON-lines records for stage files, and
// zero for asset files (each DumpFile is one asset).
type DumpFile struct {
	Path  string
	Key   string
	Size  int64
	Count int64
}

// DumpManifest is everything Dump wrote to the staging directory.
type DumpManifest struct {
	Entities      DumpFile
	Links         DumpFile
	Configuration DumpFile
	Assets        []DumpFile
}

// Dump pulls entities, links and configuration (and, if includeAssets is
// true, media assets) from Strapi and writes them as files under dir. The
// caller owns dir and is responsible for uploading and cleaning it up.
func (t *TransferClient) Dump(ctx context.Context, dir string, includeAssets bool) (DumpManifest, error) {
	s, closeSession, err := t.startSession(ctx)
	if err != nil {
		return DumpManifest{}, err
	}
	defer closeSession()

	if err := s.action(ctx, "bootstrap"); err != nil {
		return DumpManifest{}, err
	}

	manifest, err := s.pullStages(ctx, dir)
	if err != nil {
		return manifest, err
	}

	if includeAssets {
		assets, err := s.pullAssets(ctx, dir)
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
func (s *pullSession) pullStages(ctx context.Context, dir string) (DumpManifest, error) {
	var manifest DumpManifest
	for _, step := range []struct {
		name string
		dst  *DumpFile
	}{
		{transferStepEntities, &manifest.Entities},
		{transferStepLinks, &manifest.Links},
		{transferStepConfig, &manifest.Configuration},
	} {
		df, err := s.pullJSONLStep(ctx, step.name, dir)
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

	for {
		env, err := s.nextFrame(ctx)
		if err != nil {
			return wireEnvelope{}, err
		}
		if env.UUID == id {
			return env, nil
		}
		s.pending = append(s.pending, env)
	}
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

func (s *pullSession) startStep(ctx context.Context, step string) error {
	_, err := s.dispatch(ctx, wireEnvelope{
		Type: "transfer", Kind: "step", TransferID: s.transferID, Step: step, Action: "start",
	})
	return errors.Wrapf(err, "start %s step", step)
}

func (s *pullSession) endStep(ctx context.Context, step string) error {
	_, err := s.dispatch(ctx, wireEnvelope{
		Type: "transfer", Kind: "step", TransferID: s.transferID, Step: step, Action: "end",
	})
	return errors.Wrapf(err, "end %s step", step)
}

// pullJSONLStep requests one of the entities/links/configuration stages and
// writes each streamed record as a line of gzip-compressed JSON.
func (s *pullSession) pullJSONLStep(ctx context.Context, step, dir string) (DumpFile, error) {
	if err := s.startStep(ctx, step); err != nil {
		return DumpFile{}, err
	}

	path := filepath.Join(dir, step+".jsonl.gz")
	count, err := s.collectJSONLStep(ctx, step, path)
	if err != nil {
		return DumpFile{}, errors.Wrapf(err, "collect %s step", step)
	}
	if err := s.endStep(ctx, step); err != nil {
		return DumpFile{}, err
	}

	fi, err := os.Stat(path)
	if err != nil {
		return DumpFile{}, errors.Wrap(err, "stat stage file")
	}
	return DumpFile{Path: path, Key: filepath.Base(path), Size: fi.Size(), Count: count}, nil
}

var newline = []byte("\n")

func (s *pullSession) collectJSONLStep(ctx context.Context, step, path string) (int64, error) {
	f, err := os.Create(path) //nolint:gosec // path is built from a provider-managed temp dir + fixed stage name
	if err != nil {
		return 0, errors.Wrap(err, "create stage file")
	}
	defer f.Close() //nolint:errcheck
	gz := gzip.NewWriter(f)

	var count int64
	for {
		env, err := s.nextFrame(ctx)
		if err != nil {
			return count, err
		}
		if env.Type != "transfer" || env.Kind != "step" || env.Step != step {
			continue
		}
		switch env.Action {
		case "stream":
			n, err := writeJSONLBatch(gz, env.Data)
			count += n
			if err != nil {
				return count, err
			}
		case "end":
			return count, gz.Close()
		}
	}
}

func writeJSONLBatch(w *gzip.Writer, data json.RawMessage) (int64, error) {
	var items []json.RawMessage
	if err := json.Unmarshal(data, &items); err != nil {
		return 0, errors.Wrap(err, "decode stream batch")
	}
	var n int64
	for _, item := range items {
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
// the local file.
type assetMeta struct {
	Filename string `json:"filename"`
}

// assetFlowItem is one entry of an "assets" step stream batch. Each asset
// has its own start/stream/end lifecycle nested inside the outer step's
// stream messages.
type assetFlowItem struct {
	AssetID string          `json:"assetID"`
	Action  string          `json:"action"`
	Data    json.RawMessage `json:"data"`
}

func (s *pullSession) pullAssets(ctx context.Context, dir string) ([]DumpFile, error) {
	if err := s.startStep(ctx, transferStepAssets); err != nil {
		return nil, err
	}

	assetsDir := filepath.Join(dir, "assets")
	if err := os.MkdirAll(assetsDir, 0o750); err != nil {
		return nil, errors.Wrap(err, "create assets dir")
	}

	open := map[string]*os.File{}
	defer closeAll(open)

	var files []DumpFile
	for {
		env, err := s.nextFrame(ctx)
		if err != nil {
			return files, err
		}
		batch, done, err := s.handleAssetFrame(ctx, assetsDir, open, env)
		files = append(files, batch...)
		if err != nil || done {
			return files, err
		}
	}
}

// handleAssetFrame applies one frame of the assets step stream. done is true
// once the server signals the step has ended.
func (s *pullSession) handleAssetFrame(ctx context.Context, assetsDir string, open map[string]*os.File, env wireEnvelope) ([]DumpFile, bool, error) {
	if env.Type != "transfer" || env.Kind != "step" || env.Step != transferStepAssets {
		return nil, false, nil
	}
	switch env.Action {
	case "stream":
		written, err := s.applyAssetBatch(assetsDir, open, env.Data)
		return written, false, err
	case "end":
		return nil, true, s.endStep(ctx, transferStepAssets)
	default:
		return nil, false, nil
	}
}

func (s *pullSession) applyAssetBatch(dir string, open map[string]*os.File, data json.RawMessage) ([]DumpFile, error) {
	var items []assetFlowItem
	if err := json.Unmarshal(data, &items); err != nil {
		return nil, errors.Wrap(err, "decode asset batch")
	}
	var files []DumpFile
	for _, item := range items {
		df, err := applyAssetItem(dir, open, item)
		if err != nil {
			return files, err
		}
		if df != nil {
			files = append(files, *df)
		}
	}
	return files, nil
}

func applyAssetItem(dir string, open map[string]*os.File, item assetFlowItem) (*DumpFile, error) {
	switch item.Action {
	case "start":
		return nil, startAsset(dir, open, item)
	case "stream":
		return nil, streamAssetChunk(open, item)
	case "end":
		return endAsset(open, item)
	default:
		return nil, nil
	}
}

func startAsset(dir string, open map[string]*os.File, item assetFlowItem) error {
	var meta assetMeta
	_ = json.Unmarshal(item.Data, &meta) // best-effort; fall back to an assetID-only name
	name := sanitizeAssetFilename(item.AssetID, meta.Filename)
	f, err := os.Create(filepath.Join(dir, name)) //nolint:gosec // name is sanitized below
	if err != nil {
		return errors.Wrapf(err, "create asset file %q", name)
	}
	open[item.AssetID] = f
	return nil
}

func streamAssetChunk(open map[string]*os.File, item assetFlowItem) error {
	f, ok := open[item.AssetID]
	if !ok {
		return errors.Errorf("asset %q streamed before start", item.AssetID)
	}
	raw, err := decodeAssetChunk(item.Data)
	if err != nil {
		return errors.Wrapf(err, "decode asset %q chunk", item.AssetID)
	}
	if _, err := f.Write(raw); err != nil {
		return errors.Wrapf(err, "write asset %q", item.AssetID)
	}
	return nil
}

func endAsset(open map[string]*os.File, item assetFlowItem) (*DumpFile, error) {
	f, ok := open[item.AssetID]
	if !ok {
		return nil, nil
	}
	delete(open, item.AssetID)
	if err := f.Close(); err != nil {
		return nil, errors.Wrapf(err, "close asset %q", item.AssetID)
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, errors.Wrap(err, "stat asset file")
	}
	return &DumpFile{Path: f.Name(), Key: "assets/" + filepath.Base(f.Name()), Size: fi.Size()}, nil
}

// decodeAssetChunk accepts both the compact base64-string wire form
// (requested via the init command's assetEncoding param) and the legacy
// `{type:"Buffer", data:[n,n,...]}` shape sent by Strapi versions that
// predate it.
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

func closeAll(open map[string]*os.File) {
	for _, f := range open {
		_ = f.Close()
	}
}
