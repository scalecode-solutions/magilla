# Changelog

All notable changes to magilla relative to its parent project, [gorilla/websocket](https://github.com/gorilla/websocket), are recorded here. The fork point is upstream commit [`e064f32`](https://github.com/gorilla/websocket/commit/e064f32) ("Implements HTTPS proxy functionality", March 2025).

References of the form `#NNN` link to issues or pull requests on the upstream repository.

## Unreleased

### Modernization

- Bumped language version from Go 1.20 to Go 1.26.
- Bumped `golang.org/x/net` from v0.26.0 to v0.53.0, closing the path flagged by upstream issue #991.
- `interface{}` → `any` across the codebase (#1000).
- Removed the long-dead `appengine` build-tag fallback (`mask_safe.go`) and the legacy `// +build` lines.
- Marked `Conn.UnderlyingConn` properly as deprecated so godoc / linters surface it (#998).
- Typo fixes (#975).

### Correctness fixes ported from open upstream PRs

- `Upgrader.Upgrade` now actually reuses the hijacked write buffer. The pre-fork `len(buf) >= maxFrameHeaderSize+256` check was always false because `bufio.Writer.AvailableBuffer()` returns a `len==0` slice. Switched to `cap(buf)` and extended the slice (#1011, #972).
- `flate.Reader` / `flate.Writer` `Close()` is now idempotent. Double-close no longer returns a spurious `errWriteClosed` / `io.ErrClosedPipe` and no longer re-pools the flate instance (#1013, #859).
- `NextReader` no longer latches read-timeouts into a permanent `c.readErr`. A single expired `ReadDeadline` no longer poisons the connection; the caller can extend the deadline and retry. `c.readErrCount` resets on any successful frame (#1007, partial #474).
- `httpProxyDialer` sends `Proxy-Authorization` whenever a username is present, even with an empty password (RFC 7617 / #977).
- `DialContext` now merges `CookieJar` cookies with a caller-supplied `Cookie` header instead of one silently overwriting the other (#599 / #597).

### New transport: HTTP/2 (RFC 8441)

Bootstrap WebSockets over HTTP/2 via Extended CONNECT.

```go
d := &magilla.Dialer{HTTP2: magilla.HTTP2Auto}
conn, resp, err := d.Dial("wss://example.com/ws", nil)
```

- `Dialer.HTTP2` enum: `HTTP2Disabled` (default), `HTTP2Auto` (try h2, fall back to h1), `HTTP2Required`.
- Server-side: `Upgrader.Upgrade` transparently detects `CONNECT + :protocol=websocket + ProtoMajor=2` and dispatches to the h2 handler. No new methods.
- Client uses `golang.org/x/net/http2.Transport.RoundTrip` directly because `http.Client.Do` rejects the `:protocol` pseudo-header. Duplex stream wrapped as a `net.Conn` so existing framing code runs unchanged.
- Server-side flushes after every WebSocket frame to avoid the h2 write-scheduler deadlock.
- `SetDeadline` on an h2-backed `*Conn` cancels the whole stream on expiry (h2 streams have no native deadline API). Documented in `doc.go`.
- Until Go flips the default, h2 servers must be started with `GODEBUG=http2xconnect=1` to advertise `SETTINGS_ENABLE_CONNECT_PROTOCOL`.

### Concurrent-safe writes (#980, #826, #704)

`WriteMessage`, `WriteJSON`, `WritePreparedMessage`, and `NextWriter` are now safe to call from multiple goroutines without an external mutex.

- New outer `writeMu` channel-mutex serializes public write operations across their full duration.
- Inner `mu` (around `c.conn.Write`) preserved so `WriteControl` can still interleave control frames between data frames of an active streaming writer (RFC 6455 §5.5).
- Mutex acquisition honors the current write deadline — a slow writer no longer starves a sibling past its own deadline (#704).
- `c.writeDeadline` moved to `atomic.Value` so `SetWriteDeadline` is safe to call concurrently.
- **Behavior change:** `NextWriter` now holds the write lock until `Close`. The legacy "call NextWriter again to implicitly close the previous writer" shortcut is removed; applications must call Close explicitly. Documented in the Concurrency section of `doc.go`.

### Compression: RFC 7692 context-takeover (#342)

`permessage-deflate` with persistent dictionary state across messages. On a 5×-repeated 1.1 KB payload, takeover compresses to 187 bytes vs 355 without takeover (~47% smaller).

```go
u := &magilla.Upgrader{CompressionMode: magilla.CompressionModeContextTakeover}
d := &magilla.Dialer{CompressionMode:  magilla.CompressionModeContextTakeover}
```

- New `CompressionMode` enum: `Default` (defers to `EnableCompression`), `Disabled`, `NoContextTakeover`, `ContextTakeover`.
- Per-direction takeover: handshake parser respects `server_no_context_takeover` / `client_no_context_takeover` independently.
- Declines any `*_max_window_bits` ≠ 15 cleanly (Go's `compress/flate` is fixed at 15-bit windows).
- `WritePreparedMessage` returns `ErrPreparedMessageContextTakeover` on a takeover connection — pre-compressed frames are not decodable against a per-connection dictionary.
- Memory cost (~600 KB to 1.2 MB per connection of `flate.Writer` state, plus 32 KB userspace sliding window per direction) is documented in `doc.go`.

### Context-aware APIs (#474, #997, #770)

```go
mt, p, err := c.ReadMessageContext(ctx)
mt, r, err := c.NextReaderContext(ctx)
err := c.WriteMessageContext(ctx, mt, data)
err := c.WriteControlContext(ctx, mt, data)
w, err := c.NextWriterContext(ctx, mt)
```

On `ctx.Done()`, an in-flight read/write is unblocked via a past-time deadline; the resulting timeout is translated to `ctx.Err()`. Connections remain usable for subsequent reads/writes (the foundation fix that treats read timeouts as recoverable enables this). Closes the gap that was the most-cited reason for migrating away to coder/websocket.

### Handshake knobs

- `Upgrader.NegotiateSubprotocol func(r, offered) string` — pick a subprotocol based on request context instead of a static list (#606, #480).
- `Upgrader.IsValidChallengeKey func(string) bool` — accept non-spec `Sec-WebSocket-Key` formats (Nintendo Switch, other consoles) (#882).
- `Conn.SetWriteFrameSize(n)` — auto-fragment messages into ≤n-byte frames for proxy-friendliness (#814).
- `Dialer.ProxyConnectHeader http.Header` — extra headers on the CONNECT request to an HTTP proxy (#988, #605, #479).
- `Dialer.DisableClientMask bool` — opt-out of RFC 6455 client→server frame masking for backend peers that explicitly tolerate it (#985).
- `Dialer.MaxErrorBodySize int` — raise the 1024-byte cap on the failed-handshake body buffer (#994 / PR #1005).
- `*http.Response.TLS` is populated on the handshake response so callers can inspect the negotiated cipher, peer cert, etc. (#967).

### Graceful close (#487, #448)

```go
err := conn.CloseGracefully(magilla.CloseNormalClosure, "bye", deadline)
```

Encapsulates the ~15 lines of boilerplate every gorilla user writes: send a `Close` control frame, drain reads until the peer echoes its own Close, tear down the underlying `net.Conn`. Returns the first error encountered; the underlying Close is always attempted.

### Project hygiene

- Renamed module to `github.com/scalecode-solutions/magilla`.
- Renamed package to `magilla` (lowercase per Go convention).
- Removed `.circleci/` and `.github/` directories from the upstream fork.
- BSD 2-clause license preserved per its terms; original gorilla copyright headers retained on files they authored.

### Items deliberately not implemented

- **Evented read API (#481)** — audience for this self-selected to `nbio` / `lxzan/gws`. Adding a half-measure here doesn't make magilla competitive at the 1M-connection tier the request implies.
- **Negotiated `*_max_window_bits` < 15 (#661)** — would require a `klauspost/compress` dependency. Real demand is minimal; current decline-cleanly behavior is RFC-compliant.
- **WebAssembly target (#432)** — different API shape; coder/websocket already serves this niche.
- **`*http.Client` on `Dialer` (#959)** — RFC ambiguity, contentious design.
- **Server-shutdown coordination (#448 follow-up, #997)** — the right answer is application-level (track conns yourself, call `CloseGracefully` on each from your shutdown wrapper). A library-level solution would be 200+ LOC of opinion.
