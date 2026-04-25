// Copyright 2026 The magilla Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package magilla

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"
)

// HTTP2Mode controls how a Dialer negotiates the WebSocket transport.
//
// HTTP/2 support implements RFC 8441 "Bootstrapping WebSockets with HTTP/2"
// via the Extended CONNECT method. The WebSocket wire protocol above the
// transport (RFC 6455 framing, masking, close handshake, permessage-deflate)
// is identical regardless of mode.
//
// Servers must advertise SETTINGS_ENABLE_CONNECT_PROTOCOL=1 before clients
// will send Extended CONNECT. In Go 1.26 that advertisement is gated behind
// GODEBUG=http2xconnect=1 on the server process (see Go issue #71128); a
// future Go release is expected to enable it by default.
type HTTP2Mode int

const (
	// HTTP2Disabled is the default. The dialer uses the classic HTTP/1.1
	// Upgrade handshake defined in RFC 6455 §4. No HTTP/2 is attempted.
	HTTP2Disabled HTTP2Mode = iota

	// HTTP2Auto tries Extended CONNECT over HTTP/2 first. If the peer does
	// not advertise support (no SETTINGS_ENABLE_CONNECT_PROTOCOL=1) or
	// negotiates only HTTP/1.1 via ALPN, the dialer silently falls back to
	// the HTTP/1.1 Upgrade handshake.
	HTTP2Auto

	// HTTP2Required forces Extended CONNECT over HTTP/2. If the peer does
	// not support it, Dial returns an error rather than falling back.
	HTTP2Required
)

// h2StreamConn adapts an HTTP/2 stream to net.Conn so the existing RFC 6455
// framing code in conn.go can run unchanged on top of it.
//
// The read side is a plain io.Reader pulled from the stream (resp.Body on
// the client, r.Body on the server). The write side is an io.Writer plus an
// optional Flush hook; the server wraps http.ResponseWriter and calls
// http.ResponseController.Flush after each Write so frames don't sit in the
// h2 write scheduler waiting for more data. The client writes to an io.Pipe
// whose read end is the request body, so writes are naturally handed to the
// h2 transport goroutine.
//
// HTTP/2 streams do not expose a native deadline API. SetDeadline is
// implemented by a timer that cancels the stream context when the deadline
// expires; this "kills the stream" semantics matches how WebSocket callers
// use deadlines in practice (they arm a deadline around a specific I/O and
// abandon the connection on expiry). An error returned from a Read or Write
// after a deadline has fired is normalized to os.ErrDeadlineExceeded.
type h2StreamConn struct {
	r      io.Reader
	w      io.Writer
	flush  func() error       // nil or flush hook called after each Write
	cancel context.CancelFunc // cancels the request/stream context
	closer func() error       // stream-close callback run exactly once

	local  net.Addr
	remote net.Addr

	mu                sync.Mutex
	readDeadline      *time.Timer
	writeDeadline     *time.Timer
	deadlineExceeded  bool

	closeOnce sync.Once
	closeErr  error
}

func newH2StreamConn(
	r io.Reader,
	w io.Writer,
	flushFn func() error,
	cancel context.CancelFunc,
	closer func() error,
	local, remote net.Addr,
) *h2StreamConn {
	return &h2StreamConn{
		r:      r,
		w:      w,
		flush:  flushFn,
		cancel: cancel,
		closer: closer,
		local:  local,
		remote: remote,
	}
}

func (c *h2StreamConn) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if debugH2 && err != nil {
		debugLog("h2StreamConn.Read err=" + err.Error())
	}
	if err != nil && c.didDeadlineFire() {
		return n, os.ErrDeadlineExceeded
	}
	return n, err
}

func (c *h2StreamConn) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if err != nil {
		if c.didDeadlineFire() {
			return n, os.ErrDeadlineExceeded
		}
		return n, err
	}
	if c.flush != nil {
		if ferr := c.flush(); ferr != nil {
			if c.didDeadlineFire() {
				return n, os.ErrDeadlineExceeded
			}
			return n, ferr
		}
	}
	return n, nil
}

func (c *h2StreamConn) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		if c.readDeadline != nil {
			c.readDeadline.Stop()
			c.readDeadline = nil
		}
		if c.writeDeadline != nil {
			c.writeDeadline.Stop()
			c.writeDeadline = nil
		}
		c.mu.Unlock()

		if c.closer != nil {
			c.closeErr = c.closer()
		}
		if c.cancel != nil {
			c.cancel()
		}
	})
	return c.closeErr
}

func (c *h2StreamConn) LocalAddr() net.Addr  { return c.local }
func (c *h2StreamConn) RemoteAddr() net.Addr { return c.remote }

func (c *h2StreamConn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

func (c *h2StreamConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readDeadline = c.armDeadline(c.readDeadline, t)
	return nil
}

func (c *h2StreamConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeDeadline = c.armDeadline(c.writeDeadline, t)
	return nil
}

// armDeadline is called with c.mu held. It stops existing and, for a
// non-zero t, installs a new time.AfterFunc that marks the deadline as
// exceeded and cancels the stream context.
func (c *h2StreamConn) armDeadline(existing *time.Timer, t time.Time) *time.Timer {
	if existing != nil {
		existing.Stop()
	}
	if t.IsZero() {
		return nil
	}
	d := time.Until(t)
	if d <= 0 {
		c.deadlineExceeded = true
		if c.cancel != nil {
			c.cancel()
		}
		return nil
	}
	return time.AfterFunc(d, c.fireDeadline)
}

func (c *h2StreamConn) fireDeadline() {
	c.mu.Lock()
	c.deadlineExceeded = true
	c.mu.Unlock()
	if debugH2 {
		debugLog("h2StreamConn.fireDeadline: closing stream")
	}
	// Cancelling the context alone is not enough to unblock an in-flight
	// resp.Body.Read on the client side (the h2 transport only watches
	// ctx.Done() during the initial handshake, not for the ongoing stream).
	// Closing the stream via our closer callback and cancelling the
	// context are both necessary: the close unblocks readers, the cancel
	// tears down any ancillary goroutines.
	if c.closer != nil {
		_ = c.closer()
	}
	if c.cancel != nil {
		c.cancel()
	}
}

// debugH2 / debugLog are compile-time gated by the h2debug tag so we can
// wire in printf-style tracing without leaving it in production builds.
var debugH2 = false

func debugLog(msg string) {
	if debugH2 {
		// intentionally use fmt to avoid a log import dep.
		_, _ = os.Stderr.WriteString("[h2] " + msg + "\n")
	}
}

func (c *h2StreamConn) didDeadlineFire() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deadlineExceeded
}

// ErrHTTP2NotSupported is returned from Dial/DialContext when HTTP2Required
// was specified but the peer did not advertise RFC 8441 support.
var ErrHTTP2NotSupported = errors.New("websocket: HTTP/2 Extended CONNECT not supported by peer")

// isExtendedConnectNotSupported matches the unexported x/net/http2
// errExtendedConnectNotSupported by its Error() string.
//
// TODO(scalecode): once x/net/http2 exports this error (tracking Go
// issue #71128 family), switch to errors.Is with the exported symbol.
const h2ExtendedConnectNotSupportedMsg = "net/http: extended connect not supported by peer"

func isExtendedConnectNotSupported(err error) bool {
	for err != nil {
		if err.Error() == h2ExtendedConnectNotSupportedMsg {
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// isH2NotAvailable returns true for errors that indicate the peer cannot
// speak HTTP/2 Extended CONNECT, either because it didn't negotiate h2 via
// ALPN or didn't advertise SETTINGS_ENABLE_CONNECT_PROTOCOL. Used by the
// HTTP2Auto fallback path to decide whether to retry over HTTP/1.1.
//
// We intentionally do NOT swallow generic network errors (connection
// refused, context deadline, etc.) — those are real problems that should
// surface to the caller, not be papered over with an h1 retry that would
// waste time and mask the bug.
func isH2NotAvailable(err error) bool {
	if err == nil {
		return false
	}
	if isExtendedConnectNotSupported(err) {
		return true
	}
	// TLS ALPN mismatch: server doesn't speak h2.
	// crypto/tls emits "tls: no application protocol" (from the remote
	// alert) or "tls: client requested unsupported application protocols"
	// (self-inflicted). Match both.
	msg := err.Error()
	if strings.Contains(msg, "no application protocol") ||
		strings.Contains(msg, "unsupported application protocol") {
		return true
	}
	return false
}

var _ net.Conn = (*h2StreamConn)(nil)

// h2Addr is a net.Addr synthesized for an h2StreamConn. HTTP/2 streams ride
// on top of a TLS connection the websocket package doesn't own directly, so
// we report a descriptive string rather than the underlying TCP addresses.
type h2Addr struct {
	network string
	s       string
}

func (a h2Addr) Network() string { return a.network }
func (a h2Addr) String() string  { return a.s }

// errHTTP2RequiresHTTPS is returned when HTTP/2 is requested without TLS.
// RFC 8441 permits Extended CONNECT over h2c in principle, but almost
// nothing supports that in practice and the stdlib path requires TLS.
var errHTTP2RequiresHTTPS = errors.New("websocket: HTTP/2 requires an https:// URL")

// errHTTP2ProxyUnsupported is returned when a Proxy is configured alongside
// a non-Disabled HTTP2Mode. Tunneling Extended CONNECT through an HTTP
// proxy is a separate design problem we haven't tackled yet.
var errHTTP2ProxyUnsupported = errors.New("websocket: Dialer.Proxy is not supported with HTTP/2 (set HTTP2 = HTTP2Disabled)")

// dialHTTP2 establishes a WebSocket connection using RFC 8441 Extended
// CONNECT over HTTP/2. It is called from Dialer.DialContext when HTTP2 is
// Auto or Required.
//
// On success it returns a *Conn wired to an h2StreamConn, along with the
// handshake response. On failure it returns the response (if one was
// received) so callers can inspect status/headers.
//
// This function deliberately does not reuse the h1 dial path: the h2
// handshake differs in method (CONNECT), status code (200 not 101),
// headers (no Upgrade/Connection/Sec-WebSocket-Key/Accept), and in how
// the duplex stream is exposed (request-body pipe + response body).
func (d *Dialer) dialHTTP2(
	ctx context.Context,
	u *url.URL,
	requestHeader http.Header,
) (*Conn, *http.Response, error) {
	if u.Scheme != "https" {
		return nil, nil, errHTTP2RequiresHTTPS
	}
	if d.Proxy != nil {
		return nil, nil, errHTTP2ProxyUnsupported
	}

	// We call http2.Transport.RoundTrip directly rather than going through
	// http.Client / http.Transport. The high-level client rejects
	// ":protocol" as an invalid header field name before the request ever
	// reaches the h2 layer. The low-level http2.Transport accepts it and
	// hoists it to the HEADERS frame's pseudo-header section (see
	// x/net/http2 transport.go:1440).
	tlsCfg := cloneTLSConfig(d.TLSClientConfig)
	if tlsCfg.ServerName == "" {
		tlsCfg.ServerName = stripPort(u.Host)
	}
	// Offer h2 in ALPN. Keep any other protocols the caller pre-configured
	// so an h1-only server fails cleanly at TLS time (we can fall back).
	if !containsString(tlsCfg.NextProtos, "h2") {
		tlsCfg.NextProtos = append([]string{"h2"}, tlsCfg.NextProtos...)
	}

	tr := &http2.Transport{
		TLSClientConfig: tlsCfg,
	}
	// Honor a caller-supplied TLS dial if set. http2.Transport builds its
	// own TLS connection by default using TLSClientConfig; the user's
	// NetDialTLSContext takes precedence when present. A user who sets
	// only NetDialContext/NetDial (TCP) does not get h2 support — the
	// h2 transport has no way to wrap a TCP conn in TLS+ALPN internally.
	if d.NetDialTLSContext != nil {
		tr.DialTLSContext = func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
			return d.NetDialTLSContext(ctx, network, addr)
		}
	}
	defer tr.CloseIdleConnections()
	// Request body is the write side of the WebSocket stream. We hand the
	// read half to http.NewRequestWithContext; the h2 transport reads it
	// to produce DATA frames. Writes by our caller to pw are flushed
	// through the pipe synchronously.
	pr, pw := io.Pipe()

	// Stream context derives from the caller's context directly (NOT from
	// the handshake-timeout context). Cancelling the stream context on
	// Close sends RST_STREAM; we don't want the handshake timer to do
	// that to an already-established stream.
	streamCtx, streamCancel := context.WithCancel(ctx)

	// Handshake timeout: a watchdog goroutine cancels the stream context
	// if RoundTrip hasn't returned within d.HandshakeTimeout. On success
	// we close handshakeDone to retire the goroutine before ownership of
	// streamCancel transfers to the resulting h2StreamConn.
	var handshakeDone chan struct{}
	if d.HandshakeTimeout != 0 {
		handshakeDone = make(chan struct{})
		go func() {
			select {
			case <-handshakeDone:
			case <-time.After(d.HandshakeTimeout):
				streamCancel()
			}
		}()
	}

	req, err := http.NewRequestWithContext(streamCtx, http.MethodConnect, u.String(), pr)
	if err != nil {
		_ = pw.Close()
		streamCancel()
		return nil, nil, err
	}
	// http.Request normalizes URL.Scheme; we just want :scheme=https,
	// :authority=host, :path=path — which is the default for CONNECT
	// when URL is set. Nothing to override here.

	// Pseudo-header: x/net/http2 hoists ":protocol" from Header into
	// the HEADERS frame's pseudo-header section (see x/net/http2
	// transport.go:1440, go1.24+).
	req.Header.Set(":protocol", "websocket")
	req.Header["Sec-WebSocket-Version"] = []string{"13"}

	if len(d.Subprotocols) > 0 {
		req.Header["Sec-WebSocket-Protocol"] = []string{strings.Join(d.Subprotocols, ", ")}
	}
	if offer := compressionOfferHeader(effectiveCompressionMode(d.CompressionMode, d.EnableCompression)); offer != "" {
		req.Header["Sec-WebSocket-Extensions"] = []string{offer}
	}
	// Copy caller-supplied headers. Validation matches the h1 path except
	// that Upgrade / Connection / Sec-WebSocket-Key are rejected as nonsense
	// over h2 rather than as "duplicate".
	for k, vs := range requestHeader {
		switch {
		case k == "Host":
			if len(vs) > 0 {
				req.Host = vs[0]
			}
		case k == "Upgrade" || k == "Connection" || k == "Sec-Websocket-Key":
			_ = pw.Close()
			streamCancel()
			return nil, nil, errors.New("websocket: header not allowed over HTTP/2: " + k)
		case k == "Sec-Websocket-Version" ||
			k == "Sec-Websocket-Extensions" ||
			(k == "Sec-Websocket-Protocol" && len(d.Subprotocols) > 0):
			_ = pw.Close()
			streamCancel()
			return nil, nil, errors.New("websocket: duplicate header not allowed: " + k)
		case k == "Sec-Websocket-Protocol":
			req.Header["Sec-WebSocket-Protocol"] = vs
		default:
			req.Header[k] = vs
		}
	}
	// Cookies merged after caller headers (see #599).
	if d.Jar != nil {
		for _, cookie := range d.Jar.Cookies(u) {
			req.AddCookie(cookie)
		}
	}

	resp, err := tr.RoundTrip(req)
	// Retire the handshake-timeout watchdog either way; RoundTrip has
	// either produced a response or a final error.
	if handshakeDone != nil {
		close(handshakeDone)
	}
	if err != nil {
		_ = pw.Close()
		streamCancel()
		if errors.Is(err, http2.ErrNoCachedConn) {
			return nil, nil, err
		}
		if isExtendedConnectNotSupported(err) {
			return nil, nil, ErrHTTP2NotSupported
		}
		return nil, nil, err
	}

	// Persist response cookies regardless of status.
	if d.Jar != nil {
		if rc := resp.Cookies(); len(rc) > 0 {
			d.Jar.SetCookies(u, rc)
		}
	}

	// RFC 8441 §5.1: a successful handshake is 2xx. Historically we
	// accept 200 specifically; any other status aborts.
	if resp.StatusCode != http.StatusOK {
		_ = pw.Close()
		streamCancel()
		// Drain a bounded prefix of the body to aid debugging, matching
		// h1 behavior. Cap from Dialer.MaxErrorBodySize.
		limit := d.MaxErrorBodySize
		if limit <= 0 {
			limit = 1024
		}
		buf := make([]byte, limit)
		n, _ := io.ReadFull(resp.Body, buf)
		_ = resp.Body.Close()
		resp.Body = io.NopCloser(strings.NewReader(string(buf[:n])))
		return nil, resp, ErrBadHandshake
	}

	// Validate subprotocol against the dialer's list, if we offered any.
	if selected := resp.Header.Get("Sec-WebSocket-Protocol"); selected != "" {
		if !containsString(d.Subprotocols, selected) {
			_ = pw.Close()
			streamCancel()
			_ = resp.Body.Close()
			return nil, resp, ErrBadHandshake
		}
	}

	// Compression negotiation — same parser as the h1 path, including
	// per-direction context-takeover state.
	mode := effectiveCompressionMode(d.CompressionMode, d.EnableCompression)
	var (
		compress      bool
		readTakeover  bool
		writeTakeover bool
	)
	if mode != CompressionModeDisabled {
		for _, ext := range parseExtensions(resp.Header) {
			if ext[""] != "permessage-deflate" {
				continue
			}
			p := parsePMDeflate(ext)
			if !p.windowBitsValid {
				_ = pw.Close()
				streamCancel()
				_ = resp.Body.Close()
				return nil, resp, errInvalidCompression
			}
			if mode == CompressionModeNoContextTakeover && (!p.serverNoContextTakeover || !p.clientNoContextTakeover) {
				_ = pw.Close()
				streamCancel()
				_ = resp.Body.Close()
				return nil, resp, errInvalidCompression
			}
			compress = true
			readTakeover = mode == CompressionModeContextTakeover && !p.serverNoContextTakeover
			writeTakeover = mode == CompressionModeContextTakeover && !p.clientNoContextTakeover
			break
		}
	}

	// Wrap the duplex stream as a net.Conn. Close closes the pipe writer
	// (which ends our write side) and the response body (which ends the
	// stream with RST_STREAM).
	closer := func() error {
		_ = pw.Close()
		return resp.Body.Close()
	}
	local := h2Addr{network: "http2", s: "client"}
	remote := h2Addr{network: "http2", s: u.Host}
	streamConn := newH2StreamConn(resp.Body, pw, nil, streamCancel, closer, local, remote)

	c := newConn(streamConn, false, d.ReadBufferSize, d.WriteBufferSize, d.WriteBufferPool, nil, nil)
	c.disableMask = d.DisableClientMask
	c.subprotocol = resp.Header.Get("Sec-WebSocket-Protocol")
	if compress {
		if writeTakeover {
			c.writeCompressFactory = &contextTakeoverWriterFactory{}
			c.newCompressionWriter = c.writeCompressFactory.newCompressionWriter
		} else {
			c.newCompressionWriter = compressNoContextTakeover
		}
		if readTakeover {
			c.readDecompressFactory = &contextTakeoverReaderFactory{}
			c.newDecompressionReader = c.readDecompressFactory.newDecompressionReader
		} else {
			c.newDecompressionReader = decompressNoContextTakeover
		}
	}
	// Swap the response body so callers see an empty reader like the h1
	// path does (the real body is owned by streamConn now). resp.TLS was
	// already populated by http2.Transport.RoundTrip.
	resp.Body = io.NopCloser(strings.NewReader(""))
	return c, resp, nil
}

// upgradeHTTP2 handles an RFC 8441 Extended CONNECT handshake. It is called
// from Upgrader.Upgrade when the incoming request is CONNECT with
// :protocol=websocket on an HTTP/2 connection.
//
// The net/http h2 server surfaces :protocol as a normal header on r.Header
// and decodes :authority/:path into r.Host/r.URL. There is no Hijack on h2,
// so we get the duplex stream via r.Body (read) and w (write), with
// per-frame http.ResponseController.Flush to push DATA frames out promptly.
//
// Errors before we write the status code are reported via returnError
// (which writes a 4xx/5xx status). Errors after WriteHeader are returned
// as-is; by then the handshake has succeeded on the wire and the response
// stream has been committed.
func (u *Upgrader) upgradeHTTP2(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (*Conn, error) {
	const badHandshake = "websocket: the client is not using the websocket protocol: "

	if r.Header.Get("Sec-Websocket-Version") != "13" {
		return u.returnError(w, r, http.StatusBadRequest, "websocket: unsupported version: 13 not found in 'Sec-Websocket-Version' header")
	}
	if _, ok := responseHeader["Sec-Websocket-Extensions"]; ok {
		return u.returnError(w, r, http.StatusInternalServerError, "websocket: application specific 'Sec-WebSocket-Extensions' headers are unsupported")
	}

	checkOrigin := u.CheckOrigin
	if checkOrigin == nil {
		checkOrigin = checkSameOrigin
	}
	if !checkOrigin(r) {
		return u.returnError(w, r, http.StatusForbidden, "websocket: request origin not allowed by Upgrader.CheckOrigin")
	}

	// Extended CONNECT does not carry Sec-WebSocket-Key/Accept — those are
	// HTTP/1.1 artifacts of the RFC 6455 Upgrade handshake. Don't require
	// or compute them here.
	_ = badHandshake

	subprotocol := u.selectSubprotocol(r, responseHeader)

	mode := effectiveCompressionMode(u.CompressionMode, u.EnableCompression)
	var (
		compress            bool
		serverWriteTakeover bool
		clientWriteTakeover bool
	)
	if mode != CompressionModeDisabled {
		for _, ext := range parseExtensions(r.Header) {
			if ext[""] != "permessage-deflate" {
				continue
			}
			p := parsePMDeflate(ext)
			if !p.windowBitsValid {
				break
			}
			compress = true
			if mode == CompressionModeContextTakeover {
				serverWriteTakeover = !p.serverNoContextTakeover
				clientWriteTakeover = !p.clientNoContextTakeover
			}
			break
		}
	}

	// Assemble response headers.
	respH := w.Header()
	if subprotocol != "" {
		respH.Set("Sec-WebSocket-Protocol", subprotocol)
	}
	if compress {
		ext := "permessage-deflate"
		if !serverWriteTakeover {
			ext += "; server_no_context_takeover"
		}
		if !clientWriteTakeover {
			ext += "; client_no_context_takeover"
		}
		respH.Set("Sec-WebSocket-Extensions", ext)
	}
	for k, vs := range responseHeader {
		if k == "Sec-Websocket-Protocol" {
			continue
		}
		for _, v := range vs {
			respH.Add(k, sanitizeHeaderValue(v))
		}
	}

	rc := http.NewResponseController(w)
	// EnableFullDuplex is a no-op on h2 (streams are already full-duplex)
	// but harmless to call. Errors there are not fatal.
	_ = rc.EnableFullDuplex()

	w.WriteHeader(http.StatusOK)
	// Flush the HEADERS frame so the client's http.Client.Do returns and
	// starts streaming its own DATA. Without this flush the client blocks
	// indefinitely waiting for HEADERS.
	if err := rc.Flush(); err != nil {
		return nil, err
	}

	// Wrap as net.Conn. Every Write is followed by a Flush so WebSocket
	// frames don't sit in the h2 write scheduler. On close we cancel the
	// request context and close the request body; the h2 server will
	// then send RST_STREAM.
	streamCtx, streamCancel := context.WithCancel(r.Context())
	_ = streamCtx
	closer := func() error { return r.Body.Close() }
	local := h2Addr{network: "http2", s: "server"}
	remote := h2Addr{network: "http2", s: r.RemoteAddr}
	streamConn := newH2StreamConn(r.Body, w, rc.Flush, streamCancel, closer, local, remote)

	c := newConn(streamConn, true, u.ReadBufferSize, u.WriteBufferSize, u.WriteBufferPool, nil, nil)
	c.subprotocol = subprotocol
	if compress {
		if serverWriteTakeover {
			c.writeCompressFactory = &contextTakeoverWriterFactory{}
			c.newCompressionWriter = c.writeCompressFactory.newCompressionWriter
		} else {
			c.newCompressionWriter = compressNoContextTakeover
		}
		if clientWriteTakeover {
			c.readDecompressFactory = &contextTakeoverReaderFactory{}
			c.newDecompressionReader = c.readDecompressFactory.newDecompressionReader
		} else {
			c.newDecompressionReader = decompressNoContextTakeover
		}
	}
	return c, nil
}

// sanitizeHeaderValue strips control characters from a header value to
// prevent response splitting, mirroring the h1 path's inline check.
func sanitizeHeaderValue(v string) string {
	b := make([]byte, len(v))
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c <= 31 {
			c = ' '
		}
		b[i] = c
	}
	return string(b)
}

// stripPort returns host without the :port suffix, if any.
func stripPort(host string) string {
	if i := strings.LastIndex(host, ":"); i >= 0 {
		// Exclude IPv6 literals like "[::1]:443" where the last colon is
		// after a closing bracket — simplest check.
		if !strings.HasSuffix(host[:i], "]") || strings.Contains(host, "]") {
			return host[:i]
		}
	}
	return host
}

// containsString reports whether ss contains s.
func containsString(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

