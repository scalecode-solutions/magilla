# Migrating from gorilla/websocket

magilla is a maintained fork of [gorilla/websocket](https://github.com/gorilla/websocket). It is **not** a transparent drop-in: a few changes break source compatibility, a few more change behavior in ways your code will notice. This document walks every difference you might hit, organized by how much work it is to deal with.

For the canonical per-symbol API reference, use the godoc:

  https://pkg.go.dev/github.com/scalecode-solutions/magilla

For the full change log with issue references, see [CHANGELOG.md](./CHANGELOG.md).

---

## TL;DR

```
sed -i '' 's|github.com/gorilla/websocket|github.com/scalecode-solutions/magilla|g' **/*.go go.mod
sed -i '' 's|websocket\.|magilla.|g' **/*.go   # only in your files; verify diff
```

Then rebuild. If the build is clean, the only behavior changes likely to bite are:

1. **`NextWriter` requires explicit `Close()`** — no more implicit "next call closes the previous". Add `defer w.Close()` after every `NextWriter`.
2. **`TextMessage` payloads are validated as UTF-8** — if you were sending raw bytes as `TextMessage`, switch to `BinaryMessage` or set `SkipUTF8Validation: true`.
3. **`WriteMessage` from multiple goroutines no longer panics** — it Just Works. You can delete the external mutex you wrapped it in.

Everything else is either a fix to behavior gorilla had wrong, or a new opt-in feature you don't have to use.

---

## 1. Required source changes

### 1.1 Module path

| Before | After |
|---|---|
| `import "github.com/gorilla/websocket"` | `import "github.com/scalecode-solutions/magilla"` |
| `module github.com/gorilla/websocket` (`go.mod`) | (no change to your `go.mod`'s module) — only your `require` line changes: `require github.com/scalecode-solutions/magilla vX.Y.Z` |

### 1.2 Package identifier

The package is declared `magilla` (lowercase, per Go convention), not `websocket`. Every package-qualified reference in your own code changes:

```diff
- conn, err := websocket.DefaultDialer.Dial(url, nil)
+ conn, err := magilla.DefaultDialer.Dial(url, nil)

- if websocket.IsCloseError(err, websocket.CloseNormalClosure) { ... }
+ if magilla.IsCloseError(err, magilla.CloseNormalClosure) { ... }

- var u websocket.Upgrader
+ var u magilla.Upgrader
```

If you'd rather keep the `websocket.X` spelling without rewriting your codebase, use a named import:

```go
import websocket "github.com/scalecode-solutions/magilla"
```

Go's package-name resolution is unaffected; both forms work.

### 1.3 `NextWriter` requires explicit `Close()`

This is the one behavior change that will deadlock your code if you don't address it.

**Before (gorilla):** `NextWriter` returned a writer; if you forgot to `Close()` it and called `NextWriter` again on the same connection, the second call implicitly closed the first.

**After (magilla):** `NextWriter` acquires the connection's write mutex and holds it until `Close()` is called. If you forget to call `Close()`, the next write — from this goroutine or any other — deadlocks.

```diff
- w, _ := conn.NextWriter(magilla.TextMessage)
- w.Write([]byte("first"))
- // no Close — bug
- w2, _ := conn.NextWriter(magilla.TextMessage)  // BLOCKS FOREVER
+ w, _ := conn.NextWriter(magilla.TextMessage)
+ w.Write([]byte("first"))
+ w.Close()                                       // required
+ w2, _ := conn.NextWriter(magilla.TextMessage)
```

The right fix is `defer w.Close()` in almost every case:

```go
w, err := conn.NextWriter(magilla.TextMessage)
if err != nil {
    return err
}
defer w.Close()
// ... use w
```

This was the price of making concurrent writes safe by default. See [§2.3](#23-concurrent-writes-no-longer-panic).

---

## 2. Behavior changes (no code change required, but watch for them)

### 2.1 `TextMessage` with invalid UTF-8 closes the connection

**Before:** gorilla's docs said "It is the application's responsibility to ensure that text messages are valid UTF-8 encoded text." In practice, no validation happened — invalid UTF-8 was delivered to the application as-is.

**After:** magilla streams every `TextMessage` byte through a per-byte UTF-8 DFA on both the read and write side. Invalid sequences are RFC 6455 §6.1 fail-fast:

- **Reads:** `ReadMessage` / `NextReader` returns `errInvalidUTF8`, the library sends a Close frame with code `1007` (`CloseInvalidFramePayloadData`), and the connection becomes unusable.
- **Writes:** `WriteMessage(TextMessage, ...)` returns `errInvalidUTF8` *before* any wire bytes leak. `NextWriter`'s returned writer fails on the first invalid `Write` call.

If your code sends raw binary data as `TextMessage` (a spec violation, but common in the wild), you have two options:

```go
// Option A — recommended: send it as BinaryMessage
conn.WriteMessage(magilla.BinaryMessage, rawBytes)

// Option B — opt out of validation entirely
d := &magilla.Dialer{SkipUTF8Validation: true}    // client side
u := magilla.Upgrader{SkipUTF8Validation: true}   // server side
```

`BinaryMessage` payloads are *never* UTF-8 validated regardless of the flag.

### 2.2 Read timeouts no longer poison the connection

**Before:** A single expired `SetReadDeadline` cached the timeout error in `c.readErr` and every subsequent `ReadMessage` / `NextReader` call returned the same error forever. To recover you had to tear down and reconnect.

**After:** A `net.Error` with `Timeout() == true` is returned to the caller without latching `c.readErr`. The application can call `SetReadDeadline` with a fresh deadline and retry on the same connection.

```go
for {
    _ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
    _, msg, err := conn.ReadMessage()
    if err == nil {
        handle(msg)
        continue
    }
    var ne net.Error
    if errors.As(err, &ne) && ne.Timeout() {
        // safe to continue — the connection is fine
        continue
    }
    return err  // a real error
}
```

If you previously rebuilt connections after timeouts, you can stop. Other error classes (network failures, close errors) still latch as before.

### 2.3 Concurrent writes no longer panic

**Before:** gorilla's docs said "Applications are responsible for ensuring that no more than one goroutine calls the write methods concurrently." Calling `WriteMessage` from two goroutines simultaneously had a best-effort `c.isWriting` panic detector that would fire `panic("concurrent write to websocket connection")`.

**After:** `WriteMessage`, `WriteJSON`, `WritePreparedMessage`, and `NextWriter` are safe to call from multiple goroutines without an external mutex. The library serializes them on an internal write mutex, and the deadline-aware acquire respects `SetWriteDeadline` so a slow predecessor doesn't starve a fast caller (fixes upstream #704).

If you wrapped your writes in your own mutex, you can delete it:

```diff
- var mu sync.Mutex
- mu.Lock()
- err := conn.WriteMessage(magilla.TextMessage, payload)
- mu.Unlock()
+ err := conn.WriteMessage(magilla.TextMessage, payload)
```

`WriteControl` continues to use a separate inner mutex so control frames (ping, pong, close) can interleave between data frames of an active streaming writer, as RFC 6455 §5.5 permits.

### 2.4 `CookieJar` and manual `Cookie` header now merge

**Before:** If you set `Dialer.Jar` and *also* passed a `Cookie` header in `requestHeader`, one silently dropped the other depending on call order.

**After:** Jar cookies are applied after the manual headers via `req.AddCookie`, which appends to the existing `Cookie` header per RFC 6265 §5.4. Both sources end up on the wire.

No code change required if you were doing the right thing. If your code worked around the old bug by setting one source to `nil`, that workaround is now harmless.

### 2.5 Empty proxy-auth password is now sent

**Before:** A proxy URL like `http://user@proxy/` (no password) caused gorilla to skip the `Proxy-Authorization` header entirely.

**After:** RFC 7617 permits an empty password, and some corporate proxies require this exact configuration. magilla now sends `Proxy-Authorization: Basic base64(user:)` whenever a username is present.

No code change required. If you were intentionally relying on the omission to skip auth, set the proxy URL without any userinfo.

### 2.6 `*http.Response.TLS` is populated after `wss://` dials

**Before:** `gorilla` did the TLS handshake manually and didn't fill in `Response.TLS`, so callers couldn't inspect the negotiated cipher, peer certificates, or SNI from the handshake response.

**After:** `Response.TLS` is set to the underlying `*tls.Conn`'s `ConnectionState`. Available immediately after `Dial` / `DialContext` returns.

```go
conn, resp, _ := d.Dial("wss://example.com/ws", nil)
fmt.Println(resp.TLS.NegotiatedProtocol)
fmt.Println(resp.TLS.PeerCertificates[0].Subject)
```

### 2.7 `WritePreparedMessage` errors on context-takeover connections

If the connection's `CompressionMode` is `CompressionModeContextTakeover`, calling `WritePreparedMessage` returns `ErrPreparedMessageContextTakeover`. This is fundamental: a `PreparedMessage` caches a frame compressed against an empty dictionary, but a takeover connection has a per-connection dictionary that has accumulated state. The pre-compressed frame is not decodable on the receiving side.

If you use both, choose one:

```go
// Option A: stick with no-takeover, keep PreparedMessage broadcasts
u := magilla.Upgrader{CompressionMode: magilla.CompressionModeNoContextTakeover}

// Option B: takeover + per-connection compression (no PreparedMessage)
u := magilla.Upgrader{CompressionMode: magilla.CompressionModeContextTakeover}
// per-connection: conn.WriteMessage(magilla.TextMessage, payload)
```

---

## 3. New opt-in features (no migration, just FYI)

These are new in magilla. Your existing code doesn't need to use them, but they exist if you want them.

### 3.1 HTTP/2 (RFC 8441 Extended CONNECT)

```go
d := &magilla.Dialer{HTTP2: magilla.HTTP2Auto}
conn, resp, err := d.Dial("wss://example.com/ws", nil)
```

Server-side, `Upgrader.Upgrade` automatically dispatches based on `r.ProtoMajor`. No code change.

**Important:** until Go flips its default, h2 servers must be started with `GODEBUG=http2xconnect=1` to advertise `SETTINGS_ENABLE_CONNECT_PROTOCOL`. Without it, h2 clients silently fall back to h1.

### 3.2 Context-takeover compression (RFC 7692)

```go
u := magilla.Upgrader{CompressionMode: magilla.CompressionModeContextTakeover}
```

Persistent compression dictionary across messages. ~47% smaller payloads in our tests on repetitive content (chat, telemetry). Costs ~600 KB to 1.2 MB per connection of `flate.Writer` state.

`EnableCompression: true` (without `CompressionMode`) continues to select `CompressionModeNoContextTakeover`, which matches gorilla's default behavior byte-for-byte.

### 3.3 Context-aware reads and writes

```go
mt, p, err := conn.ReadMessageContext(ctx)
mt, r, err := conn.NextReaderContext(ctx)
err  := conn.WriteMessageContext(ctx, mt, data)
err  := conn.WriteControlContext(ctx, mt, data)
w, err := conn.NextWriterContext(ctx, mt)
```

On `ctx.Done()`, the in-flight operation aborts with `ctx.Err()`. The connection remains usable for subsequent reads/writes — no need to tear down.

### 3.4 `CloseGracefully`

```go
err := conn.CloseGracefully(magilla.CloseNormalClosure, "bye", deadline)
```

One method that does the full RFC 6455 close handshake: send Close, drain reads until peer echoes (or deadline hits), tear down `net.Conn`. Replaces ~15 lines of boilerplate.

### 3.5 Handshake / framing knobs

| Field | What |
|---|---|
| `Upgrader.NegotiateSubprotocol func(*http.Request, []string) string` | Pick a subprotocol based on request context instead of a static list |
| `Upgrader.IsValidChallengeKey func(string) bool` | Custom `Sec-WebSocket-Key` validator (Nintendo Switch et al.) |
| `Conn.SetWriteFrameSize(n)` | Auto-fragment messages into ≤n-byte frames |
| `Dialer.ProxyConnectHeader http.Header` | Extra headers on the `CONNECT` to an HTTP proxy |
| `Dialer.DisableClientMask bool` | Skip RFC 6455 client→server frame masking (backend peers only) |
| `Dialer.MaxErrorBodySize int` | Raise the 1024-byte cap on the failed-handshake body buffer |

### 3.6 Removed

Nothing material was removed. The `appengine` build-tag fallback (`mask_safe.go`) is gone — Classic App Engine retired in 2020 and modern App Engine uses the standard Go runtime, so this code path was unreachable.

---

## 4. Common patterns side-by-side

### 4.1 The classic chat-style connection

```go
// Before (gorilla)
conn, err := upgrader.Upgrade(w, r, nil)
if err != nil { return }
defer conn.Close()

for {
    mt, msg, err := conn.ReadMessage()
    if err != nil { break }
    if err := conn.WriteMessage(mt, msg); err != nil { break }
}
```

The same code works on magilla, with one improvement available:

```go
// After (magilla) — drop-in if you change the import
conn, err := upgrader.Upgrade(w, r, nil)
if err != nil { return }
defer func() {
    _ = conn.CloseGracefully(magilla.CloseNormalClosure, "", time.Now().Add(time.Second))
}()

for {
    mt, msg, err := conn.ReadMessage()
    if err != nil { break }
    if err := conn.WriteMessage(mt, msg); err != nil { break }
}
```

### 4.2 Streaming writer — be sure to Close

```go
// Before (gorilla, also valid on magilla)
w, _ := conn.NextWriter(magilla.TextMessage)
w.Write([]byte("part 1\n"))
w.Write([]byte("part 2\n"))
// implicit close on next NextWriter — no longer works on magilla

// After (magilla — required)
w, err := conn.NextWriter(magilla.TextMessage)
if err != nil { return err }
defer w.Close()
w.Write([]byte("part 1\n"))
w.Write([]byte("part 2\n"))
```

### 4.3 Concurrent writers

```go
// Before (gorilla)
var mu sync.Mutex
go func() {
    mu.Lock()
    conn.WriteMessage(magilla.TextMessage, []byte("from goroutine A"))
    mu.Unlock()
}()
go func() {
    mu.Lock()
    conn.WriteMessage(magilla.TextMessage, []byte("from goroutine B"))
    mu.Unlock()
}()

// After (magilla — delete the mutex)
go func() {
    conn.WriteMessage(magilla.TextMessage, []byte("from goroutine A"))
}()
go func() {
    conn.WriteMessage(magilla.TextMessage, []byte("from goroutine B"))
}()
```

### 4.4 Cancellable read loop

```go
// Before (gorilla)
go func() {
    for {
        select {
        case <-ctx.Done():
            conn.Close()  // crude — corrupts in-flight reads
            return
        default:
        }
        _, msg, err := conn.ReadMessage()
        // ...
    }
}()

// After (magilla)
for {
    _, msg, err := conn.ReadMessageContext(ctx)
    if err != nil {
        if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
            return
        }
        return err
    }
    handle(msg)
}
```

---

## 5. What we did *not* change

Some things in gorilla worked fine and we left them alone:

- All exported types: `Conn`, `Dialer`, `Upgrader`, `CloseError`, `HandshakeError`, `PreparedMessage`, `BufferPool` — same shape, same methods (modulo the additions above).
- All close codes (`CloseNormalClosure` etc.) and message types (`TextMessage`, `BinaryMessage`, etc.).
- Message-handler API: `SetCloseHandler`, `SetPingHandler`, `SetPongHandler`.
- The compression-level API: `SetCompressionLevel`, `EnableWriteCompression`.
- `IsCloseError`, `IsUnexpectedCloseError`, `FormatCloseMessage`, `Subprotocols`, `IsWebSocketUpgrade`.

If you were relying on any of these, your code keeps working unchanged.

---

## Reporting issues

If something here doesn't match your migration experience, or you hit a behavior change we didn't document, please open an issue at https://github.com/scalecode-solutions/magilla/issues with the gorilla/websocket version you came from and a minimal reproducer.
