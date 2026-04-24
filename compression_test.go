package magilla

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }

func TestTruncWriter(t *testing.T) {
	const data = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijlkmnopqrstuvwxyz987654321"
	for n := 1; n <= 10; n++ {
		var b bytes.Buffer
		w := &truncWriter{w: nopCloser{&b}}
		p := []byte(data)
		for len(p) > 0 {
			m := len(p)
			if m > n {
				m = n
			}
			_, _ = w.Write(p[:m])
			p = p[m:]
		}
		if b.String() != data[:len(data)-len(w.p)] {
			t.Errorf("%d: %q", n, b.String())
		}
	}
}

func textMessages(num int) [][]byte {
	messages := make([][]byte, num)
	for i := 0; i < num; i++ {
		msg := fmt.Sprintf("planet: %d, country: %d, city: %d, street: %d", i, i, i, i)
		messages[i] = []byte(msg)
	}
	return messages
}

func BenchmarkWriteNoCompression(b *testing.B) {
	w := io.Discard
	c := newTestConn(nil, w, false)
	messages := textMessages(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.WriteMessage(TextMessage, messages[i%len(messages)])
	}
	b.ReportAllocs()
}

func BenchmarkWriteWithCompression(b *testing.B) {
	w := io.Discard
	c := newTestConn(nil, w, false)
	messages := textMessages(100)
	c.enableWriteCompression = true
	c.newCompressionWriter = compressNoContextTakeover
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.WriteMessage(TextMessage, messages[i%len(messages)])
	}
	b.ReportAllocs()
}

func TestValidCompressionLevel(t *testing.T) {
	c := newTestConn(nil, nil, false)
	for _, level := range []int{minCompressionLevel - 1, maxCompressionLevel + 1} {
		if err := c.SetCompressionLevel(level); err == nil {
			t.Errorf("no error for level %d", level)
		}
	}
	for _, level := range []int{minCompressionLevel, maxCompressionLevel} {
		if err := c.SetCompressionLevel(level); err != nil {
			t.Errorf("error for level %d", level)
		}
	}
}

// --- context-takeover tests ---

// setupPermessageDeflateConn builds a pair of client/server Conns wired
// via net.Pipe, with both sides configured for the given CompressionMode.
// The handshake itself is skipped (both sides are hand-configured); this
// isolates the compression/decompression state machine from HTTP
// handshake concerns.
func setupPermessageDeflateConn(t *testing.T, mode CompressionMode) (client, server *Conn) {
	t.Helper()
	cConn, sConn := net.Pipe()
	client = newConn(cConn, false, 4096, 4096, nil, nil, nil)
	server = newConn(sConn, true, 4096, 4096, nil, nil, nil)

	apply := func(c *Conn) {
		c.enableWriteCompression = true
		c.compressionLevel = defaultCompressionLevel
		switch mode {
		case CompressionModeNoContextTakeover:
			c.newCompressionWriter = compressNoContextTakeover
			c.newDecompressionReader = decompressNoContextTakeover
		case CompressionModeContextTakeover:
			c.writeCompressFactory = &contextTakeoverWriterFactory{}
			c.newCompressionWriter = c.writeCompressFactory.newCompressionWriter
			c.readDecompressFactory = &contextTakeoverReaderFactory{}
			c.newDecompressionReader = c.readDecompressFactory.newDecompressionReader
		}
	}
	apply(client)
	apply(server)
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return client, server
}

// TestCompressionContextTakeover_RoundTrip verifies that a repetitive
// payload sent multiple times over a context-takeover connection
// round-trips correctly.
func TestCompressionContextTakeover_RoundTrip(t *testing.T) {
	client, server := setupPermessageDeflateConn(t, CompressionModeContextTakeover)

	const iterations = 5
	// Highly repetitive payload — ideal for LZ77 back-references to
	// kick in across messages under takeover.
	msg := []byte(strings.Repeat("the quick brown fox jumps over the lazy dog. ", 40))

	recv := make(chan []byte, iterations)
	go func() {
		for i := 0; i < iterations; i++ {
			_, data, err := server.ReadMessage()
			if err != nil {
				t.Errorf("server ReadMessage #%d: %v", i, err)
				close(recv)
				return
			}
			recv <- data
		}
		close(recv)
	}()

	for i := 0; i < iterations; i++ {
		if err := client.WriteMessage(TextMessage, msg); err != nil {
			t.Fatalf("client WriteMessage #%d: %v", i, err)
		}
	}

	got := 0
	for data := range recv {
		if !bytes.Equal(data, msg) {
			t.Fatalf("message %d: mismatch", got)
		}
		got++
	}
	if got != iterations {
		t.Fatalf("received %d messages, want %d", got, iterations)
	}
}

// TestCompressionContextTakeover_ImprovesRatio sends the same repetitive
// payload through a no-takeover path and a takeover path, and asserts
// that the takeover path compresses significantly better. This exercises
// the core promise of the feature: dictionary reuse across messages.
func TestCompressionContextTakeover_ImprovesRatio(t *testing.T) {
	// Use a plain bytes.Buffer as the wire; measure bytes written per
	// message. We write 5 identical large repetitive messages and sum
	// compressed output.
	msg := []byte(strings.Repeat("metrics: cpu=0.5, mem=2048, load=0.1; ", 30))
	const iterations = 5

	measure := func(mode CompressionMode) int {
		var buf bytes.Buffer
		c := newConn(fakeNetConn{Reader: nil, Writer: &buf}, false, 4096, 4096, nil, nil, nil)
		c.enableWriteCompression = true
		c.compressionLevel = defaultCompressionLevel
		switch mode {
		case CompressionModeNoContextTakeover:
			c.newCompressionWriter = compressNoContextTakeover
		case CompressionModeContextTakeover:
			c.writeCompressFactory = &contextTakeoverWriterFactory{}
			c.newCompressionWriter = c.writeCompressFactory.newCompressionWriter
		}
		for i := 0; i < iterations; i++ {
			if err := c.WriteMessage(TextMessage, msg); err != nil {
				t.Fatalf("%v WriteMessage #%d: %v", mode, i, err)
			}
		}
		return buf.Len()
	}

	noTakeover := measure(CompressionModeNoContextTakeover)
	takeover := measure(CompressionModeContextTakeover)
	t.Logf("compressed bytes: no-takeover=%d takeover=%d", noTakeover, takeover)

	// Expect takeover to be at least 25% smaller on this repetitive input.
	// Real-world ratios are much larger; 25% is a floor to avoid flakes
	// from compression-level or flate-version variance across Go versions.
	if takeover >= noTakeover*3/4 {
		t.Errorf("context-takeover did not compress better: no-takeover=%d takeover=%d", noTakeover, takeover)
	}
}

// TestCompressionContextTakeover_PreparedMessage verifies that
// WritePreparedMessage on a takeover connection returns
// ErrPreparedMessageContextTakeover rather than silently producing
// undecodable output.
func TestCompressionContextTakeover_PreparedMessage(t *testing.T) {
	c := newTestConn(nil, &bytes.Buffer{}, false)
	c.enableWriteCompression = true
	c.compressionLevel = defaultCompressionLevel
	c.writeCompressFactory = &contextTakeoverWriterFactory{}
	c.newCompressionWriter = c.writeCompressFactory.newCompressionWriter

	pm, err := NewPreparedMessage(TextMessage, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	err = c.WritePreparedMessage(pm)
	if err != ErrPreparedMessageContextTakeover {
		t.Fatalf("got %v, want ErrPreparedMessageContextTakeover", err)
	}

	// And: a non-data message (close/ping/pong) is NOT subject to the
	// guard, because control messages aren't compressed anyway. Ping
	// goes through WriteControl, not WritePreparedMessage; check a
	// zero-byte TextMessage still trips the guard.
	pmEmpty, _ := NewPreparedMessage(TextMessage, nil)
	if err := c.WritePreparedMessage(pmEmpty); err != ErrPreparedMessageContextTakeover {
		t.Fatalf("empty TextMessage: got %v, want ErrPreparedMessageContextTakeover", err)
	}
}

// TestCompressionContextTakeover_HandshakeNegotiation spins up a real
// HTTP server with Upgrader configured for takeover and a Dialer
// configured for takeover, and verifies that the response headers and
// Conn state line up correctly.
func TestCompressionContextTakeover_HandshakeNegotiation(t *testing.T) {
	cases := []struct {
		name                 string
		serverMode           CompressionMode
		clientMode           CompressionMode
		wantHeader           string // expected server response Sec-WebSocket-Extensions
		wantClientWriteFactory bool
		wantClientReadFactory  bool
	}{
		{
			name:       "server=takeover, client=takeover",
			serverMode: CompressionModeContextTakeover,
			clientMode: CompressionModeContextTakeover,
			// Server responds with no no-takeover params → both sides takeover.
			wantHeader:             "permessage-deflate",
			wantClientWriteFactory: true,
			wantClientReadFactory:  true,
		},
		{
			name:       "server=notakeover, client=takeover",
			serverMode: CompressionModeNoContextTakeover,
			clientMode: CompressionModeContextTakeover,
			// Server forces no-takeover on both directions.
			wantHeader: "permessage-deflate; server_no_context_takeover; client_no_context_takeover",
		},
		{
			name:       "server=takeover, client=notakeover",
			serverMode: CompressionModeContextTakeover,
			clientMode: CompressionModeNoContextTakeover,
			// Client's offer demands both no-takeover params; server MUST
			// honor server_no_context_takeover, and since
			// client_no_context_takeover was offered as a hint the server
			// echoes it too (our implementation respects client's hint
			// when the client explicitly sent it).
			wantHeader: "permessage-deflate; server_no_context_takeover; client_no_context_takeover",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := &Upgrader{CompressionMode: tc.serverMode}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, err := u.Upgrade(w, r, nil)
				if err != nil {
					t.Logf("upgrade: %v", err)
					return
				}
				defer c.Close()
				for {
					if _, _, err := c.ReadMessage(); err != nil {
						return
					}
				}
			}))
			defer srv.Close()

			wsURL, _ := url.Parse(srv.URL)
			wsURL.Scheme = "ws"
			d := &Dialer{CompressionMode: tc.clientMode}
			c, resp, err := d.Dial(wsURL.String(), nil)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			defer c.Close()

			got := resp.Header.Get("Sec-Websocket-Extensions")
			if got != tc.wantHeader {
				t.Errorf("response Sec-WebSocket-Extensions = %q, want %q", got, tc.wantHeader)
			}
			if tc.wantClientWriteFactory != (c.writeCompressFactory != nil) {
				t.Errorf("client writeCompressFactory present = %v, want %v", c.writeCompressFactory != nil, tc.wantClientWriteFactory)
			}
			if tc.wantClientReadFactory != (c.readDecompressFactory != nil) {
				t.Errorf("client readDecompressFactory present = %v, want %v", c.readDecompressFactory != nil, tc.wantClientReadFactory)
			}
		})
	}
}

// TestCompressionContextTakeover_WindowBitsRejected verifies that a
// client offering a non-15 windowBits value causes the server to decline
// the permessage-deflate extension entirely.
func TestCompressionContextTakeover_WindowBitsRejected(t *testing.T) {
	u := &Upgrader{
		CompressionMode: CompressionModeContextTakeover,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := u.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("upgrade: %v", err)
			return
		}
		defer c.Close()
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	// Dial manually so we can send a custom Sec-WebSocket-Extensions
	// header that the standard Dialer wouldn't emit.
	tcpURL, _ := url.Parse(srv.URL)
	conn, err := net.DialTimeout("tcp", tcpURL.Host, 2*time.Second)
	if err != nil {
		t.Fatalf("dial tcp: %v", err)
	}
	defer conn.Close()

	req := "GET / HTTP/1.1\r\n" +
		"Host: " + tcpURL.Host + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
		"Sec-WebSocket-Version: 13\r\n" +
		"Sec-WebSocket-Extensions: permessage-deflate; server_max_window_bits=10\r\n" +
		"\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	respBuf := make([]byte, 4096)
	n, _ := conn.Read(respBuf)
	resp := string(respBuf[:n])

	if !strings.Contains(resp, "101 Switching Protocols") {
		t.Fatalf("handshake did not complete: %q", resp)
	}
	if strings.Contains(resp, "Sec-WebSocket-Extensions") {
		t.Errorf("server echoed permessage-deflate despite invalid windowBits: %q", resp)
	}
}
