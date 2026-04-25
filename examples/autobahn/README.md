# Autobahn Test Server

A test server for the [Autobahn WebSocket Test Suite](https://github.com/crossbario/autobahn-testsuite). Run the suite to validate magilla's RFC 6455 + RFC 7692 conformance.

See [RESULTS.md](./RESULTS.md) for the latest run summary (zero protocol failures across ~2,200 sub-cases).

## Running the suite

```bash
go run server.go &
mkdir -p reports
docker run --rm \
    -v "$(pwd)/config:/config" \
    -v "$(pwd)/reports:/reports" \
    --add-host=host.docker.internal:host-gateway \
    crossbario/autobahn-testsuite \
    wstest -m fuzzingclient -s /config/fuzzingclient.json
```

The full HTML report is written to `reports/index.html`. The fuzzer takes 10–15 minutes against five endpoint variants exercising different read/write API combinations.

`--add-host=host.docker.internal:host-gateway` is needed on Linux; Docker Desktop on macOS / Windows resolves it automatically.
