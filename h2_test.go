// Copyright 2026 The Magilla Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package magilla

import (
	"bytes"
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	_ "unsafe"

	"golang.org/x/net/http2"
)

// Force-enable RFC 8441 Extended CONNECT on the x/net/http2 server we use
// for test fixtures. The flag is normally toggled by GODEBUG=http2xconnect=1
// at process start (see Go issue #71128); reaching into the unexported var
// via linkname is a test-only escape hatch so the library's own tests
// don't require a global env variable.
//
// We deliberately only touch x/net/http2 (not the stdlib bundled copy in
// net/http). Go 1.23+ rejects linknames into net/http. Tests work around
// this by registering x/net/http2 on a plain http.Server via
// http2.ConfigureServer; that path uses x/net throughout and never
// invokes the stdlib bundled h2.
//
//go:linkname h2DisableExtendedConnect golang.org/x/net/http2.disableExtendedConnectProtocol
var h2DisableExtendedConnect bool

func init() {
	h2DisableExtendedConnect = false
}

// --- test helpers ---

// newH2TestServer spins up an httptest.Server that speaks HTTP/2 via
// x/net/http2 (explicitly, not the stdlib bundled h2) so our linkname
// override takes effect. Returns the server and the wss:// URL.
func newH2TestServer(t *testing.T, u *Upgrader, handle func(t *testing.T, c *Conn)) (*httptest.Server, string) {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := u.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("server upgrade err: %v", err)
			return
		}
		defer c.Close()
		handle(t, c)
	})
	s := httptest.NewUnstartedServer(handler)
	// Register x/net/http2 explicitly on the Server's TLSNextProto, so
	// ALPN "h2" routes to x/net's h2 stack (where our linkname points)
	// rather than the stdlib bundled copy in h2_bundle.go.
	if err := http2.ConfigureServer(s.Config, &http2.Server{}); err != nil {
		t.Fatalf("http2.ConfigureServer: %v", err)
	}
	// StartTLS sets NextProtos on the tls.Config from srv.TLSNextProto
	// keys, so "h2" will be advertised in ALPN.
	s.TLS = &tls.Config{NextProtos: []string{"h2", "http/1.1"}}
	s.StartTLS()
	t.Cleanup(s.Close)
	wsURL := "wss://" + strings.TrimPrefix(s.URL, "https://")
	return s, wsURL
}

// newH1TestServer spins up an httptest.Server WITHOUT HTTP/2 enabled.
func newH1TestServer(t *testing.T, u *Upgrader, handle func(t *testing.T, c *Conn)) (*httptest.Server, string) {
	t.Helper()
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := u.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("server upgrade err: %v", err)
			return
		}
		defer c.Close()
		handle(t, c)
	}))
	s.StartTLS()
	t.Cleanup(s.Close)
	wsURL := "wss://" + strings.TrimPrefix(s.URL, "https://")
	return s, wsURL
}

// dialerForTest returns a Dialer that trusts the httptest.Server's self-signed
// cert and uses the requested HTTP2Mode.
func dialerForTest(mode HTTP2Mode) *Dialer {
	return &Dialer{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         nil, // let Protocols/HTTP2 flag decide
		},
		HTTP2:            mode,
		HandshakeTimeout: 5 * time.Second,
	}
}

// --- echo tests ---

func TestHTTP2_Echo(t *testing.T) {
	_, url := newH2TestServer(t, &Upgrader{}, echoHandler)

	c, resp, err := dialerForTest(HTTP2Auto).Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("expected h2 handshake response, got ProtoMajor=%d", resp.ProtoMajor)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	for _, msg := range []string{"hello", "world", strings.Repeat("x", 1024)} {
		if err := c.WriteMessage(TextMessage, []byte(msg)); err != nil {
			t.Fatalf("write %q: %v", msg, err)
		}
		_, got, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(got) != msg {
			t.Errorf("roundtrip: got %q, want %q", got, msg)
		}
	}
}

func TestHTTP2_LargePayload(t *testing.T) {
	_, url := newH2TestServer(t, &Upgrader{}, echoHandler)
	c, _, err := dialerForTest(HTTP2Auto).Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	// 256KB — exceeds default h2 per-stream flow-control window so we
	// exercise WINDOW_UPDATE interaction.
	payload := bytes.Repeat([]byte{'a', 'b', 'c', 'd'}, 64*1024)
	if err := c.WriteMessage(BinaryMessage, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, got, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("payload mismatch (len %d vs %d)", len(got), len(payload))
	}
}

// --- close handshake ---

func TestHTTP2_CloseHandshake(t *testing.T) {
	serverClosed := make(chan struct{})
	_, url := newH2TestServer(t, &Upgrader{}, func(t *testing.T, c *Conn) {
		defer close(serverClosed)
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	})

	c, _, err := dialerForTest(HTTP2Auto).Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	closeMsg := FormatCloseMessage(CloseNormalClosure, "bye")
	if err := c.WriteMessage(CloseMessage, closeMsg); err != nil {
		t.Fatalf("write close: %v", err)
	}
	_ = c.Close()

	select {
	case <-serverClosed:
	case <-time.After(3 * time.Second):
		t.Fatalf("server did not observe close within deadline")
	}
}

// --- ping / pong ---

func TestHTTP2_PingPong(t *testing.T) {
	_, url := newH2TestServer(t, &Upgrader{}, func(t *testing.T, c *Conn) {
		// Server just reads; the default pong handler will reply.
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	})

	c, _, err := dialerForTest(HTTP2Auto).Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	pongCh := make(chan string, 1)
	c.SetPongHandler(func(data string) error {
		select {
		case pongCh <- data:
		default:
		}
		return nil
	})

	// Kick a goroutine to drive reads so pongs get delivered.
	go func() {
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}()

	if err := c.WriteControl(PingMessage, []byte("ping-payload"), time.Now().Add(2*time.Second)); err != nil {
		t.Fatalf("write ping: %v", err)
	}
	select {
	case got := <-pongCh:
		if got != "ping-payload" {
			t.Errorf("pong payload: got %q, want %q", got, "ping-payload")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no pong within deadline")
	}
}

// --- compression (permessage-deflate) ---

func TestHTTP2_Compression(t *testing.T) {
	_, url := newH2TestServer(t, &Upgrader{EnableCompression: true}, echoHandler)

	d := dialerForTest(HTTP2Auto)
	d.EnableCompression = true
	c, resp, err := d.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if resp.Header.Get("Sec-WebSocket-Extensions") == "" {
		t.Fatalf("expected negotiated Sec-WebSocket-Extensions")
	}

	// Send highly compressible payload; correctness is what we test, not
	// the compression ratio.
	payload := bytes.Repeat([]byte("compressible"), 4096)
	if err := c.WriteMessage(BinaryMessage, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, got, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

// --- subprotocol negotiation ---

func TestHTTP2_Subprotocol(t *testing.T) {
	_, url := newH2TestServer(t,
		&Upgrader{Subprotocols: []string{"chat-v2", "chat-v1"}},
		echoHandler)

	d := dialerForTest(HTTP2Auto)
	d.Subprotocols = []string{"chat-v1", "chat-v2"}
	c, _, err := d.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	// Gorilla's selectSubprotocol walks the client's list first and
	// returns the first one that appears anywhere in the server's list,
	// so client preference wins: "chat-v1" here.
	if got := c.Subprotocol(); got != "chat-v1" {
		t.Errorf("subprotocol: got %q, want %q", got, "chat-v1")
	}
}

// --- mode matrix ---

func TestHTTP2_AutoFallsBackToH1(t *testing.T) {
	// HTTP2Auto against an h1-only server must fall back cleanly.
	_, url := newH1TestServer(t, &Upgrader{}, echoHandler)

	c, resp, err := dialerForTest(HTTP2Auto).Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if resp.ProtoMajor != 1 {
		t.Errorf("expected h1 fallback, got ProtoMajor=%d", resp.ProtoMajor)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Errorf("expected 101, got %d", resp.StatusCode)
	}

	if err := c.WriteMessage(TextMessage, []byte("fallback")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, got, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "fallback" {
		t.Errorf("got %q, want %q", got, "fallback")
	}
}

func TestHTTP2_RequiredFailsOnH1(t *testing.T) {
	_, url := newH1TestServer(t, &Upgrader{}, echoHandler)

	_, _, err := dialerForTest(HTTP2Required).Dial(url, nil)
	if err == nil {
		t.Fatal("expected error when HTTP2Required hits h1 server")
	}
	// We don't mandate a specific sentinel here because the TLS layer may
	// report ALPN failure as a generic TLS error; any non-nil err is a
	// correct outcome.
	t.Logf("expected error (ok): %v", err)
}

func TestHTTP2_DisabledPrefersH1(t *testing.T) {
	// HTTP2Disabled against an h2-capable server must still use the h1
	// Upgrade path. httptest with EnableHTTP2=true advertises both h2 and
	// h1 in ALPN, so the h1 path should negotiate h1.
	_, url := newH2TestServer(t, &Upgrader{}, echoHandler)

	c, resp, err := dialerForTest(HTTP2Disabled).Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if resp.ProtoMajor != 1 {
		t.Errorf("expected h1, got ProtoMajor=%d", resp.ProtoMajor)
	}

	if err := c.WriteMessage(TextMessage, []byte("h1-only")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, got, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "h1-only" {
		t.Errorf("got %q, want %q", got, "h1-only")
	}
}

// --- deadline ---

func TestHTTP2_ReadDeadlineCancelsBlockedRead(t *testing.T) {
	// Server accepts but never writes.
	_, url := newH2TestServer(t, &Upgrader{}, func(t *testing.T, c *Conn) {
		// Block reading indefinitely; client never writes.
		_, _, _ = c.ReadMessage()
	})

	c, _, err := dialerForTest(HTTP2Auto).Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	start := time.Now()
	_, _, err = c.ReadMessage()
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected ReadMessage error after deadline, got nil")
	}
	if elapsed > 2*time.Second {
		t.Errorf("deadline took too long to fire: %v", elapsed)
	}
	t.Logf("deadline-triggered error (ok): %v after %v", err, elapsed)
}

// --- rejection paths ---

func TestHTTP2_RejectsNonHTTPSURL(t *testing.T) {
	d := dialerForTest(HTTP2Required)
	_, _, err := d.Dial("ws://127.0.0.1:1/ws", nil)
	if err == nil {
		t.Fatal("expected error for ws:// with HTTP2Required")
	}
	if !errors.Is(err, errHTTP2RequiresHTTPS) {
		t.Errorf("wrong error: got %v, want errHTTP2RequiresHTTPS", err)
	}
}

func TestHTTP2_RejectsProxyField(t *testing.T) {
	d := dialerForTest(HTTP2Required)
	d.Proxy = http.ProxyFromEnvironment
	_, _, err := d.Dial("wss://example.invalid/ws", nil)
	if err == nil {
		t.Fatal("expected error when Proxy is set with HTTP2Required")
	}
	if !errors.Is(err, errHTTP2ProxyUnsupported) {
		t.Errorf("wrong error: got %v, want errHTTP2ProxyUnsupported", err)
	}
}

// echoHandler is a conservative server-side echo used by most tests.
func echoHandler(t *testing.T, c *Conn) {
	for {
		mt, msg, err := c.ReadMessage()
		if err != nil {
			return
		}
		if err := c.WriteMessage(mt, msg); err != nil {
			return
		}
	}
}

