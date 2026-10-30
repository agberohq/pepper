package pepper

import (
	"context"
	"testing"
	"time"

	"github.com/agberohq/pepper/internal/bus"
	"github.com/agberohq/pepper/internal/codec"
	"github.com/agberohq/pepper/internal/envelope"
	"github.com/agberohq/pepper/internal/registry"
	"github.com/agberohq/pepper/internal/wire"
	"github.com/olekukonko/ll"
)

// newCapLoadTestPepper builds a Pepper wired to an in-memory bus, without
// starting workers or the router — just enough to exercise cap_load routing.
func newCapLoadTestPepper(t *testing.T, caps map[string]string) (*Pepper, *bus.Mock) {
	t.Helper()

	c, err := codec.Get(string(CodecMsgPack))
	if err != nil {
		t.Fatalf("codec.Get: %v", err)
	}
	m := bus.NewMock()
	pp := &Pepper{
		cfg:    defaultConfig(),
		codec:  c,
		reg:    registry.New(),
		logger: ll.New("test"),
		rt:     &runtimeState{bus: m},
	}
	for name, path := range caps {
		if err := pp.Register(Script(name, path)); err != nil {
			t.Fatalf("Register(%s): %v", name, err)
		}
	}
	return pp, m
}

// TestSendCapLoadsRoutesByRuntime guards the runtime-kind dispatch: a worker
// must only receive cap_load for capabilities of its own runtime. Before this
// was centralised, only Python was ever dispatched to, so Julia and Node workers
// connected but never loaded anything.
func TestSendCapLoadsRoutesByRuntime(t *testing.T) {
	cases := []struct {
		name        string
		announced   string
		wantCap     string
		notWantCaps []string
	}{
		{
			name:        "python worker gets python caps",
			announced:   string(registry.RuntimePython),
			wantCap:     "py.cap",
			notWantCaps: []string{"node.cap", "jl.cap"},
		},
		{
			name:        "empty runtime defaults to python",
			announced:   "",
			wantCap:     "py.cap",
			notWantCaps: []string{"node.cap", "jl.cap"},
		},
		{
			name:        "node worker gets node caps",
			announced:   string(registry.RuntimeNode),
			wantCap:     "node.cap",
			notWantCaps: []string{"py.cap", "jl.cap"},
		},
		{
			name:        "rust worker gets rust caps",
			announced:   string(registry.RuntimeRust),
			wantCap:     "rs.cap",
			notWantCaps: []string{"py.cap", "node.cap", "jl.cap"},
		},
		{
			name:        "julia worker gets julia caps",
			announced:   string(registry.RuntimeJulia),
			wantCap:     "jl.cap",
			notWantCaps: []string{"py.cap", "node.cap"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pp, m := newCapLoadTestPepper(t, map[string]string{
				"py.cap":   "./caps/echo.py",
				"node.cap": "./caps/search.js",
				"jl.cap":   "./caps/embed.jl",
				"rs.cap":   "./caps/rerank.rs",
			})

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			const wid = "w-1"
			ch, err := m.Subscribe(ctx, bus.TopicControl(wid))
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}

			pp.sendCapLoads(wid, envelope.Data{envelope.KeyGroups: []string{wire.GroupDefault}}, tc.announced)

			got, ok := nextTopicCapture(ctx, ch)
			if !ok {
				t.Fatalf("no cap_load published for runtime %q", tc.announced)
			}
			if got.MsgType != envelope.MsgCapLoad {
				t.Errorf("msg_type = %q, want %q", got.MsgType, envelope.MsgCapLoad)
			}
			if got.Cap != tc.wantCap {
				t.Errorf("cap = %q, want %q", got.Cap, tc.wantCap)
			}
			for _, unwanted := range tc.notWantCaps {
				if got.Cap == unwanted {
					t.Errorf("cap %q should not be dispatched to a %q worker", unwanted, tc.announced)
				}
			}

			// Exactly one capability should have been sent. Give it a short
			// window — a stray second cap_load arrives immediately if present.
			drainCtx, drainCancel := context.WithTimeout(ctx, 150*time.Millisecond)
			defer drainCancel()
			if extra, more := nextTopicCapture(drainCtx, ch); more {
				t.Errorf("unexpected extra cap_load for cap %q", extra.Cap)
			}
		})
	}
}

// nextTopicCapture decodes one envelope from ch, reporting false on timeout.
func nextTopicCapture(ctx context.Context, ch <-chan bus.Message) (envelope.Envelope, bool) {
	select {
	case msg, ok := <-ch:
		if !ok {
			return envelope.Envelope{}, false
		}
		var env envelope.Envelope
		if err := codec.MsgPack.Unmarshal(msg.Data, &env); err != nil {
			return envelope.Envelope{}, false
		}
		return env, true
	case <-ctx.Done():
		return envelope.Envelope{}, false
	}
}
