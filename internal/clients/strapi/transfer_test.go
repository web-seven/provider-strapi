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
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// fakeTransferServer simulates Strapi's remote pull endpoint closely enough
// to exercise the protocol: each pushed frame blocks until the client
// acknowledges it (as Strapi's confirm() does), and a step's first frame is
// pushed before the reply to the step's start request.
type fakeTransferServer struct {
	stages   map[string][]any // step -> batches pushed before the end frame
	failStep string           // step whose end frame carries an error

	mu   sync.Mutex
	acks int
	errs []string
}

func newFakeTransferServer(t *testing.T, stages map[string][]any, failStep string) (*fakeTransferServer, string) {
	t.Helper()
	f := &fakeTransferServer{stages: stages, failStep: failStep}
	upgrader := websocket.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc(transferPullPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			f.fail("upgrade: %v", err)
			return
		}
		defer conn.Close() //nolint:errcheck
		f.serve(conn)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return f, srv.URL
}

func (f *fakeTransferServer) fail(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs = append(f.errs, fmt.Sprintf(format, args...))
}

func (f *fakeTransferServer) check(t *testing.T) (acks int) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.errs {
		t.Error(e)
	}
	return f.acks
}

func (f *fakeTransferServer) serve(conn *websocket.Conn) {
	for {
		var msg wireEnvelope
		if err := conn.ReadJSON(&msg); err != nil {
			return
		}
		switch {
		case msg.Command == "init":
			f.write(conn, map[string]any{"uuid": msg.UUID, "data": map[string]any{"transferID": "transfer-1"}})
		case msg.Command == "end":
			f.write(conn, map[string]any{"uuid": msg.UUID, "data": map[string]any{"ok": true}})
			return
		case msg.Kind == "step" && msg.Action == "start":
			if !f.flush(conn, msg.UUID, msg.Step) {
				return
			}
		default: // transfer actions and step end
			f.write(conn, map[string]any{"uuid": msg.UUID, "data": map[string]any{"ok": true}})
		}
	}
}

// flush pushes a step's frames, replying to the start request right after
// the first one, and waits for each frame to be acknowledged.
func (f *fakeTransferServer) flush(conn *websocket.Conn, startUUID, step string) bool {
	id := "flush-" + step
	frames := make([]map[string]any, 0, len(f.stages[step])+1)
	for _, batch := range f.stages[step] {
		frames = append(frames, map[string]any{"type": "transfer", "data": batch, "ended": false, "error": nil, "id": id})
	}
	end := map[string]any{"type": "transfer", "data": nil, "ended": true, "error": nil, "id": id}
	if step == f.failStep {
		end["error"] = map[string]any{"message": "stage failed"}
	}
	frames = append(frames, end)

	for i, frame := range frames {
		key := fmt.Sprintf("%s-%d", step, i)
		f.write(conn, map[string]any{"uuid": key, "data": frame})
		if i == 0 {
			f.write(conn, map[string]any{"uuid": startUUID, "data": map[string]any{"ok": true, "id": id}})
		}
		var ack wireEnvelope
		if err := conn.ReadJSON(&ack); err != nil {
			return false
		}
		if ack.UUID != key {
			f.fail("expected acknowledgement of %q, got %+v", key, ack)
			return false
		}
		f.mu.Lock()
		f.acks++
		f.mu.Unlock()
	}
	return true
}

func (f *fakeTransferServer) write(conn *websocket.Conn, v any) {
	if err := conn.WriteJSON(v); err != nil {
		f.fail("write: %v", err)
	}
}

type memObject struct {
	bytes.Buffer
	closed bool
}

func (o *memObject) Close() error {
	o.closed = true
	return nil
}

type memSink struct {
	objects map[string]*memObject
}

func (m *memSink) Create(_ context.Context, key string) (io.WriteCloser, error) {
	o := &memObject{}
	m.objects[key] = o
	return o, nil
}

func gunzipLines(t *testing.T, o *memObject) []string {
	t.Helper()
	if o == nil {
		t.Fatal("object was not written")
	}
	zr, err := gzip.NewReader(bytes.NewReader(o.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) == 0 {
		return nil
	}
	return strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
}

func dumpCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestDump_StreamsStagesAndAcknowledgesFrames(t *testing.T) {
	f, endpoint := newFakeTransferServer(t, map[string][]any{
		transferStepEntities: {
			[]any{map[string]any{"id": 1}, map[string]any{"id": 2}},
			[]any{map[string]any{"id": 3}},
		},
		transferStepLinks: {map[string]any{"left": 1}}, // a single item rather than an array
		transferStepAssets: {[]any{
			map[string]any{"action": "start", "assetID": "a1", "data": map[string]any{"filename": "image.png"}},
			map[string]any{"action": "stream", "assetID": "a1", "data": map[string]any{"type": "Buffer", "data": []int{104, 105}}},
			map[string]any{"action": "end", "assetID": "a1"},
		}},
	}, "")
	c, err := NewTransferClient(TransferConfig{Endpoint: endpoint, Token: "token"})
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{objects: map[string]*memObject{}}

	manifest, err := c.Dump(dumpCtx(t), sink, true)
	if err != nil {
		t.Fatal(err)
	}

	if got := gunzipLines(t, sink.objects["entities.jsonl.gz"]); strings.Join(got, "|") != `{"id":1}|{"id":2}|{"id":3}` {
		t.Fatalf("unexpected entities: %v", got)
	}
	if got := gunzipLines(t, sink.objects["links.jsonl.gz"]); len(got) != 1 || got[0] != `{"left":1}` {
		t.Fatalf("unexpected links: %v", got)
	}
	if got := gunzipLines(t, sink.objects["configuration.jsonl.gz"]); len(got) != 0 {
		t.Fatalf("unexpected configuration: %v", got)
	}
	if manifest.Entities.Count != 3 || manifest.Links.Count != 1 || manifest.Configuration.Count != 0 {
		t.Fatalf("unexpected counts: %+v", manifest)
	}
	if manifest.Entities.Size != int64(sink.objects["entities.jsonl.gz"].Len()) {
		t.Fatalf("entities size %d does not match %d bytes written", manifest.Entities.Size, sink.objects["entities.jsonl.gz"].Len())
	}
	asset := sink.objects["assets/a1_image.png"]
	if asset == nil || asset.String() != "hi" {
		t.Fatalf("unexpected asset object: %v", asset)
	}
	if len(manifest.Assets) != 1 || manifest.Assets[0].Key != "assets/a1_image.png" || manifest.Assets[0].Size != 2 {
		t.Fatalf("unexpected assets manifest: %+v", manifest.Assets)
	}
	for key, o := range sink.objects {
		if !o.closed {
			t.Errorf("object %q was not closed", key)
		}
	}
	// entities: 2 batches + end, links: 1 + end, configuration: end, assets: 1 + end.
	if acks := f.check(t); acks != 8 {
		t.Fatalf("expected 8 acknowledged frames, got %d", acks)
	}
}

func TestDump_StageErrorFails(t *testing.T) {
	f, endpoint := newFakeTransferServer(t, map[string][]any{}, transferStepConfig)
	c, err := NewTransferClient(TransferConfig{Endpoint: endpoint, Token: "token"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.Dump(dumpCtx(t), &memSink{objects: map[string]*memObject{}}, false)
	if err == nil || !strings.Contains(err.Error(), "stage failed") {
		t.Fatalf("expected the stage error to propagate, got %v", err)
	}
	f.check(t)
}

func TestDump_RejectedToken(t *testing.T) {
	_, endpoint := newFakeTransferServer(t, nil, "")
	c, err := NewTransferClient(TransferConfig{Endpoint: endpoint, Token: "wrong"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.Dump(dumpCtx(t), &memSink{objects: map[string]*memObject{}}, false)
	if !IsTransferUnauthorized(err) {
		t.Fatalf("expected an unauthorized error, got %v", err)
	}
}
