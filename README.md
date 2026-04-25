# magilla

> *a maintained fork of [gorilla/websocket](https://github.com/gorilla/websocket) — derby and bowtie required, shirt optional.* 🦍

## What's different from gorilla/websocket

- Go 1.26 baseline, `golang.org/x/net` up to date
- **RFC 8441 WebSocket over HTTP/2** (Extended CONNECT) — client and server
- Correctness fixes for hijacked write buffer reuse, idempotent compression `Close()`, recoverable read timeouts, empty proxy auth passwords, and CookieJar/Cookie header merging
- `interface{}` → `any`, typos fixed, deprecations marked, appengine compat removed

## Install

```
go get github.com/scalecode-solutions/magilla
```

## Usage

```go
import "github.com/scalecode-solutions/magilla"

func handler(w http.ResponseWriter, r *http.Request) {
    var u magilla.Upgrader
    conn, err := u.Upgrade(w, r, nil)
    if err != nil {
        return
    }
    defer conn.Close()
    // ... use conn
}
```

For HTTP/2 (RFC 8441), set `Dialer.HTTP2 = magilla.HTTP2Auto` (client) and run your server with `GODEBUG=http2xconnect=1` until Go flips the default. See the package godoc for the full story.

## License

BSD 2-clause. Derivative of gorilla/websocket; original copyright preserved in `LICENSE` and file-level headers.
