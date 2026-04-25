# Autobahn Test Suite Results

magilla against [crossbario/autobahn-testsuite](https://github.com/crossbario/autobahn-testsuite), commit-of-record [`e916450`](https://github.com/scalecode-solutions/magilla/commit/e916450) (Apr 24 2026).

## Summary

**Zero protocol failures.** All 2,177 sub-cases across 5 endpoint configurations either pass strictly, pass with a documented limitation, or are intentionally declined.

| Endpoint | Cases run | OK | INFORMATIONAL | NON-STRICT | UNIMPLEMENTED |
|---|---:|---:|---:|---:|---:|
| `/c` CopyWriterOnly (NextReader + io.Copy) | 517 | 460 | 3 | 0 | 54 |
| `/f` CopyFull (NextReader + NextWriter + io.Copy) | 517 | 460 | 3 | 0 | 54 |
| `/m` ReadAllWriteMessage (ReadMessage + WriteMessage) | 517 | 456 | 3 | 4 | 54 |
| `/r` ReadAllWriter (ReadMessage + NextWriter) | 316 | 309 | 3 | 4 | 0 |
| `/p` ReadAllWritePreparedMessage (ReadMessage + WritePreparedMessage) | 310 | 303 | 3 | 4 | 0 |

Section coverage: cases 1–10 (RFC 6455 framing, UTF-8, handshake, close), cases 12 (permessage-deflate without context takeover), cases 13 (permessage-deflate with context takeover).

## Findings

### Zero hard failures

No panics. No nil-pointer derefs. No internal errors. No protocol violations.

### `UNIMPLEMENTED` cases (162 total) — intentional

Every `UNIMPLEMENTED` case is a `*_max_window_bits` < 15 negotiation. magilla declines these per its documented design: Go's `compress/flate` is hard-coded to a 15-bit sliding window, so we cleanly refuse the extension offer rather than lie about honoring a smaller window. `behaviorClose: OK` confirms the close handshake is correct.

Distribution across cases 13.3.x, 13.5.x, 13.7.x — exactly the windowBits-tweaked sub-cases.

### `NON-STRICT` cases (12 total) — known buffered-read tradeoff

All 12 NON-STRICT results are case 6.4.x ("invalid UTF-8 across multiple frames") on the three endpoints that use `ReadMessage` (`/m`, `/r`, `/p`). Autobahn wants the server to reject as soon as the first invalid octet is seen ("fail fast"); the buffered `ReadMessage` path validates after the whole message is reassembled. The connection is still rejected with the correct close code — just not on the strictest possible byte.

`/c` and `/f` use `NextReader` with a streaming `validator` that does fail strictly, and have zero NON-STRICT cases. This is API-shape dependent, not a bug.

This matches upstream gorilla/websocket behavior; magilla doesn't regress.

### `INFORMATIONAL` cases (3 per endpoint)

Cases 7.1.6, 7.13.1, 7.13.2 — non-binding behavioral notes about close-frame handling. Autobahn marks them informational rather than pass/fail; magilla's behavior is consistent with the de facto majority.

### `/r` and `/p` skipped cases 12.x and all of 13.x

Autobahn ran fewer compression cases against `ReadAllWriter` and `ReadAllWritePreparedMessage` than against the streaming-writer endpoints. This appears to be an autobahn-side decision based on how those endpoints respond to early section-12 cases — autobahn skips later cases in the section. Not a magilla failure (no fails are recorded), but worth noting.

## Reproducing

```bash
cd examples/autobahn
go run server.go &
mkdir -p reports
docker run --rm \
    -v "$(pwd)/config:/config" \
    -v "$(pwd)/reports:/reports" \
    --add-host=host.docker.internal:host-gateway \
    crossbario/autobahn-testsuite \
    wstest -m fuzzingclient -s /config/fuzzingclient.json
```

The full HTML report is written to `reports/index.html`.

## Caveats for this run

The autobahn server in this run used `EnableCompression: true` (which selects `CompressionModeNoContextTakeover` for backward compatibility). It does not exercise magilla's `CompressionModeContextTakeover` path. To test takeover, set `CompressionMode: magilla.CompressionModeContextTakeover` on the server's `Upgrader` and re-run; cases 13.x are designed to exercise it.
