// Copyright 2013 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package magilla

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

var subprotocolTests = []struct {
	h         string
	protocols []string
}{
	{"", nil},
	{"foo", []string{"foo"}},
	{"foo,bar", []string{"foo", "bar"}},
	{"foo, bar", []string{"foo", "bar"}},
	{" foo, bar", []string{"foo", "bar"}},
	{" foo, bar ", []string{"foo", "bar"}},
}

func TestSubprotocols(t *testing.T) {
	for _, st := range subprotocolTests {
		r := http.Request{Header: http.Header{"Sec-Websocket-Protocol": {st.h}}}
		protocols := Subprotocols(&r)
		if !reflect.DeepEqual(st.protocols, protocols) {
			t.Errorf("SubProtocols(%q) returned %#v, want %#v", st.h, protocols, st.protocols)
		}
	}
}

var isWebSocketUpgradeTests = []struct {
	ok bool
	h  http.Header
}{
	{false, http.Header{"Upgrade": {"websocket"}}},
	{false, http.Header{"Connection": {"upgrade"}}},
	{true, http.Header{"Connection": {"upgRade"}, "Upgrade": {"WebSocket"}}},
}

func TestIsWebSocketUpgrade(t *testing.T) {
	for _, tt := range isWebSocketUpgradeTests {
		ok := IsWebSocketUpgrade(&http.Request{Header: tt.h})
		if tt.ok != ok {
			t.Errorf("IsWebSocketUpgrade(%v) returned %v, want %v", tt.h, ok, tt.ok)
		}
	}
}

func TestSubProtocolSelection(t *testing.T) {
	upgrader := Upgrader{
		Subprotocols: []string{"foo", "bar", "baz"},
	}

	r := http.Request{Header: http.Header{"Sec-Websocket-Protocol": {"foo", "bar"}}}
	s := upgrader.selectSubprotocol(&r, nil)
	if s != "foo" {
		t.Errorf("Upgrader.selectSubprotocol returned %v, want %v", s, "foo")
	}

	r = http.Request{Header: http.Header{"Sec-Websocket-Protocol": {"bar", "foo"}}}
	s = upgrader.selectSubprotocol(&r, nil)
	if s != "bar" {
		t.Errorf("Upgrader.selectSubprotocol returned %v, want %v", s, "bar")
	}

	r = http.Request{Header: http.Header{"Sec-Websocket-Protocol": {"baz"}}}
	s = upgrader.selectSubprotocol(&r, nil)
	if s != "baz" {
		t.Errorf("Upgrader.selectSubprotocol returned %v, want %v", s, "baz")
	}

	r = http.Request{Header: http.Header{"Sec-Websocket-Protocol": {"quux"}}}
	s = upgrader.selectSubprotocol(&r, nil)
	if s != "" {
		t.Errorf("Upgrader.selectSubprotocol returned %v, want %v", s, "empty string")
	}
}

func TestUpgraderIsValidChallengeKey(t *testing.T) {
	cases := []struct {
		name      string
		key       string
		validator func(string) bool
		wantOK    bool
	}{
		{
			name:      "default rejects non-16-byte key",
			key:       "NOT-A-VALID-KEY",
			validator: nil,
			wantOK:    false,
		},
		{
			name:      "custom validator accepts short key",
			key:       "nintendo-switch",
			validator: func(s string) bool { return s != "" },
			wantOK:    true,
		},
		{
			name:      "custom validator rejects empty",
			key:       "",
			validator: func(s string) bool { return s != "" },
			wantOK:    false,
		},
		{
			name:      "custom validator may allow base64 shorter than 16 bytes",
			key:       "c2hvcnQ=",
			validator: func(s string) bool { return s == "c2hvcnQ=" },
			wantOK:    true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := &Upgrader{
				IsValidChallengeKey: tc.validator,
				CheckOrigin:         func(r *http.Request) bool { return true },
			}

			req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
			req.Header.Set("Upgrade", "websocket")
			req.Header.Set("Connection", "upgrade")
			req.Header.Set("Sec-Websocket-Version", "13")
			req.Header.Set("Sec-WebSocket-Key", tc.key)

			rec := httptest.NewRecorder()
			_, err := u.Upgrade(rec, req, nil)
			// We can't complete a real upgrade without a hijackable
			// ResponseWriter, so the key-validation result surfaces
			// either as "key rejected → 400" or as a post-validation
			// error about hijack failure.
			if tc.wantOK {
				if rec.Code == http.StatusBadRequest {
					t.Errorf("validator accepted key but Upgrade returned 400: %v", err)
				}
			} else {
				if rec.Code != http.StatusBadRequest {
					t.Errorf("validator rejected key but Upgrade returned %d (err=%v)", rec.Code, err)
				}
			}
		})
	}
}

func TestNegotiateSubprotocolCallback(t *testing.T) {
	// Callback overrides Subprotocols entirely.
	var gotOffered []string
	upgrader := Upgrader{
		Subprotocols: []string{"from-static-list"}, // must be ignored
		NegotiateSubprotocol: func(r *http.Request, offered []string) string {
			gotOffered = append([]string(nil), offered...)
			// Pick whichever the caller wants regardless of what the
			// static list says.
			for _, p := range offered {
				if p == "v2-authed" {
					return p
				}
			}
			return ""
		},
	}

	r := http.Request{Header: http.Header{"Sec-Websocket-Protocol": {"v1, v2-authed"}}}
	s := upgrader.selectSubprotocol(&r, nil)
	if s != "v2-authed" {
		t.Errorf("selectSubprotocol = %q, want %q", s, "v2-authed")
	}
	if len(gotOffered) != 2 || gotOffered[0] != "v1" || gotOffered[1] != "v2-authed" {
		t.Errorf("callback got offered %v, want [v1 v2-authed]", gotOffered)
	}

	// Callback can decline by returning "".
	upgrader.NegotiateSubprotocol = func(r *http.Request, offered []string) string {
		return ""
	}
	r = http.Request{Header: http.Header{"Sec-Websocket-Protocol": {"anything"}}}
	if got := upgrader.selectSubprotocol(&r, nil); got != "" {
		t.Errorf("declined callback returned %q, want empty", got)
	}
}

var checkSameOriginTests = []struct {
	ok bool
	r  *http.Request
}{
	{false, &http.Request{Host: "example.org", Header: map[string][]string{"Origin": {"https://other.org"}}}},
	{true, &http.Request{Host: "example.org", Header: map[string][]string{"Origin": {"https://example.org"}}}},
	{true, &http.Request{Host: "Example.org", Header: map[string][]string{"Origin": {"https://example.org"}}}},
}

func TestCheckSameOrigin(t *testing.T) {
	for _, tt := range checkSameOriginTests {
		ok := checkSameOrigin(tt.r)
		if tt.ok != ok {
			t.Errorf("checkSameOrigin(%+v) returned %v, want %v", tt.r, ok, tt.ok)
		}
	}
}

type reuseTestResponseWriter struct {
	brw *bufio.ReadWriter
	http.ResponseWriter
}

func (resp *reuseTestResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return fakeNetConn{strings.NewReader(""), &bytes.Buffer{}}, resp.brw, nil
}

var bufioReuseTests = []struct {
	n     int
	reuse bool
}{
	{4096, true},
	{128, false},
}

func xTestBufioReuse(t *testing.T) {
	for i, tt := range bufioReuseTests {
		br := bufio.NewReaderSize(strings.NewReader(""), tt.n)
		bw := bufio.NewWriterSize(&bytes.Buffer{}, tt.n)
		resp := &reuseTestResponseWriter{
			brw: bufio.NewReadWriter(br, bw),
		}
		upgrader := Upgrader{}
		c, err := upgrader.Upgrade(resp, &http.Request{
			Method: http.MethodGet,
			Header: http.Header{
				"Upgrade":               []string{"websocket"},
				"Connection":            []string{"upgrade"},
				"Sec-Websocket-Key":     []string{"dGhlIHNhbXBsZSBub25jZQ=="},
				"Sec-Websocket-Version": []string{"13"},
			}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if reuse := c.br == br; reuse != tt.reuse {
			t.Errorf("%d: buffered reader reuse=%v, want %v", i, reuse, tt.reuse)
		}
		writeBuf := bw.AvailableBuffer()
		if reuse := &c.writeBuf[0] == &writeBuf[0]; reuse != tt.reuse {
			t.Errorf("%d: write buffer reuse=%v, want %v", i, reuse, tt.reuse)
		}
	}
}

func TestHijack_NotSupported(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "http://example.com", nil)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "upgrade")
	req.Header.Set("Sec-Websocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	req.Header.Set("Sec-Websocket-Version", "13")

	recorder := httptest.NewRecorder()

	upgrader := Upgrader{}
	_, err := upgrader.Upgrade(recorder, req, nil)

	if want := (HandshakeError{}); !errors.As(err, &want) || recorder.Code != http.StatusInternalServerError {
		t.Errorf("want %T and status_code=%d", want, http.StatusInternalServerError)
		t.Fatalf("got err=%T and status_code=%d", err, recorder.Code)
	}
}
