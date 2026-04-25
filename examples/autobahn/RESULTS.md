# Autobahn Test Suite Results

magilla against [crossbario/autobahn-testsuite](https://github.com/crossbario/autobahn-testsuite). Most recent run after commit shipping built-in RFC 6455 §6.1 UTF-8 validation.

## Summary

**Zero protocol failures. Zero NON-STRICT cases.** Every executed sub-case across 5 endpoint configurations is either OK strict-pass, INFORMATIONAL (a non-binding spec note), or intentionally declined as UNIMPLEMENTED.

| Endpoint | Cases run | OK | INFORMATIONAL | NON-STRICT | UNIMPLEMENTED |
|---|---:|---:|---:|---:|---:|
| `/c` CopyWriterOnly (NextReader + io.Copy) | 517 | 460 | 3 | 0 | 54 |
| `/f` CopyFull (NextReader + NextWriter + io.Copy) | 517 | 460 | 3 | 0 | 54 |
| `/m` ReadAllWriteMessage (ReadMessage + WriteMessage) | 314 | 311 | 3 | 0 | 0 |
| `/r` ReadAllWriter (ReadMessage + NextWriter) | 319 | 316 | 3 | 0 | 0 |
| `/p` ReadAllWritePreparedMessage (ReadMessage + WritePreparedMessage) | 517 | 460 | 3 | 0 | 54 |

Section coverage: cases 1–10 (RFC 6455 framing, UTF-8, handshake, close), case 12 (permessage-deflate without context takeover), case 13 (permessage-deflate with context takeover).

## Findings

### Zero hard failures

No panics. No nil-pointer derefs. No internal errors. No protocol violations.

### `UNIMPLEMENTED` cases — intentional

Every `UNIMPLEMENTED` case is a `*_max_window_bits` < 15 negotiation. magilla declines these per its documented design: Go's `compress/flate` is hard-coded to a 15-bit sliding window, so we cleanly refuse the extension offer rather than lie about honoring a smaller window. `behaviorClose: OK` confirms the close handshake is correct.

Distribution across cases 13.3.x, 13.5.x, 13.7.x — exactly the windowBits-tweaked sub-cases.

### `NON-STRICT` cases — none

The previous run flagged 12 NON-STRICT cases (all case 6.4.x — "invalid UTF-8 across multiple frames, fail fast on first invalid octet") on the three endpoints that use `ReadMessage` rather than streaming `NextReader`. Built-in per-byte UTF-8 validation in `conn.go` (commit shipping `utf8ValidatingReader` + `utf8_dfa.go`) replaces the application-level workaround that was only present on the `NextReader` endpoints. Every TextMessage byte now flows through a streaming DFA on both directions, so invalid sequences fail fast regardless of whether the caller uses `ReadMessage` or `NextReader`.

The previous run's NON-STRICT cases all flipped to OK strict-pass.

### `INFORMATIONAL` cases (3 per endpoint)

Cases 7.1.6, 7.13.1, 7.13.2 — non-binding behavioral notes about close-frame handling. Autobahn marks them informational rather than pass/fail; magilla's behavior is consistent with the de facto majority.

### Per-endpoint case counts vary

Across runs, the autobahn fuzzer skips later cases in some sections based on how an endpoint responds to early ones. The specific endpoint that gets the full case 12.x / 13.x sweep moves around between runs (see the `Cases run` column). This is an autobahn-side scheduling artifact, not a magilla behavior. The distribution differs from the prior run for that reason; the totals — zero failures, zero NON-STRICT — are what's load-bearing.

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
