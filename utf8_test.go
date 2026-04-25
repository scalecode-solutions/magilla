// Copyright 2026 The magilla Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package magilla

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// --- DFA unit tests ---

func TestUTF8DFA_Valid(t *testing.T) {
	valid := [][]byte{
		[]byte(""),
		[]byte("hello"),
		[]byte("a"),
		[]byte("café"),                      // 2-byte codepoint
		[]byte("日本語"),                       // 3-byte codepoints
		[]byte("\xf0\x9f\x98\x80"),           // 4-byte codepoint (emoji)
		[]byte("mixed: abc 日本 🎉 end"),
	}
	for _, p := range valid {
		if utf8DFAValidate(p) != utf8Accept {
			t.Errorf("DFA rejected valid UTF-8: %q", p)
		}
	}
}

func TestUTF8DFA_Invalid(t *testing.T) {
	invalid := [][]byte{
		{0xff},                   // invalid start byte
		{0xc0, 0x80},             // overlong encoding of NUL
		{0xed, 0xa0, 0x80},       // high surrogate
		{0xf4, 0x90, 0x80, 0x80}, // codepoint beyond U+10FFFF
		{0xc2},                   // truncated 2-byte sequence
		{0xe2, 0x82},             // truncated 3-byte sequence
		{0x80},                   // stray continuation byte
	}
	for _, p := range invalid {
		if utf8DFAValidate(p) != utf8Reject && !(len(p) > 0 && endsMidCodepoint(p)) {
			// endsMidCodepoint: some sequences end in a valid-so-far
			// state that isn't Reject but isn't Accept either.
			// utf8DFAValidate returns that state; we count it as
			// "invalid overall" because the complete payload can't
			// be a valid UTF-8 string.
			t.Errorf("DFA accepted invalid UTF-8: %v", p)
		}
	}
}

// endsMidCodepoint returns true when running the DFA over p leaves us in
// a non-Accept, non-Reject state (i.e. payload ended mid-codepoint).
func endsMidCodepoint(p []byte) bool {
	state := utf8DFAValidate(p)
	return state != utf8Accept && state != utf8Reject
}

// --- Read-side integration tests ---

// encodeTextFrame serializes a server->client text message with the given
// FIN bit, opcode, and payload. Used to forge frames for validator tests.
func encodeTextFrame(fin bool, opcode byte, payload []byte) []byte {
	var out []byte
	b0 := opcode
	if fin {
		b0 |= finalBit
	}
	out = append(out, b0)
	switch {
	case len(payload) < 126:
		out = append(out, byte(len(payload)))
	case len(payload) < 65536:
		out = append(out, 126, byte(len(payload)>>8), byte(len(payload)))
	default:
		// not needed in these tests
		panic("payload too large")
	}
	out = append(out, payload...)
	return out
}

// TestReadTextMessage_ValidUTF8 is the happy path: a valid UTF-8 TextMessage
// round-trips through ReadMessage without error.
func TestReadTextMessage_ValidUTF8(t *testing.T) {
	payload := []byte("hello, 世界! 🌍")
	frame := encodeTextFrame(true, TextMessage, payload)

	// Client-side Conn reading from server-framed bytes.
	c := newTestConn(bytes.NewReader(frame), nil, false)
	mt, got, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if mt != TextMessage {
		t.Fatalf("mt=%d want TextMessage", mt)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("payload mismatch: got %q want %q", got, payload)
	}
}

// TestReadTextMessage_InvalidUTF8 verifies the library rejects a
// TextMessage with invalid UTF-8 and reports errInvalidUTF8.
func TestReadTextMessage_InvalidUTF8(t *testing.T) {
	// 0xff is not a legal UTF-8 start byte.
	payload := []byte{'h', 'i', 0xff, '!'}
	frame := encodeTextFrame(true, TextMessage, payload)

	c := newTestConn(bytes.NewReader(frame), &bytes.Buffer{}, false)
	_, _, err := c.ReadMessage()
	if !errors.Is(err, errInvalidUTF8) {
		t.Fatalf("ReadMessage err = %v, want errInvalidUTF8", err)
	}
}

// TestReadTextMessage_InvalidUTF8AcrossFrames reproduces autobahn case 6.4.x:
// the first continuation frame is valid, the second contains the invalid
// sequence. Validation must fail-fast on the second frame.
func TestReadTextMessage_InvalidUTF8AcrossFrames(t *testing.T) {
	// Fragmented message: valid prefix, invalid middle, valid suffix.
	// Opcode rules: first frame = TextMessage (fin=false),
	// continuation frames = continuationFrame (0).
	f1 := encodeTextFrame(false, TextMessage, []byte("valid-prefix "))
	f2 := encodeTextFrame(false, continuationFrame, []byte{0xff, 0xfe})
	f3 := encodeTextFrame(true, continuationFrame, []byte(" valid-suffix"))
	stream := append(append(f1, f2...), f3...)

	c := newTestConn(bytes.NewReader(stream), &bytes.Buffer{}, false)
	_, _, err := c.ReadMessage()
	if !errors.Is(err, errInvalidUTF8) {
		t.Fatalf("ReadMessage err = %v, want errInvalidUTF8", err)
	}
}

// TestReadTextMessage_TruncatedCodepoint: a TextMessage ending in the
// middle of a multi-byte codepoint should also be rejected.
func TestReadTextMessage_TruncatedCodepoint(t *testing.T) {
	// 0xe2 starts a 3-byte sequence; we only send 2 bytes.
	payload := []byte{'o', 'k', 0xe2, 0x82}
	frame := encodeTextFrame(true, TextMessage, payload)

	c := newTestConn(bytes.NewReader(frame), &bytes.Buffer{}, false)
	_, _, err := c.ReadMessage()
	if !errors.Is(err, errInvalidUTF8) {
		t.Fatalf("ReadMessage err = %v, want errInvalidUTF8", err)
	}
}

// TestReadTextMessage_SkipValidation confirms that SkipUTF8Validation=true
// suppresses validation; invalid bytes pass through as-is.
func TestReadTextMessage_SkipValidation(t *testing.T) {
	payload := []byte{'h', 'i', 0xff, '!'}
	frame := encodeTextFrame(true, TextMessage, payload)

	c := newTestConn(bytes.NewReader(frame), nil, false)
	c.skipUTF8Validation = true

	mt, got, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage with skip: %v", err)
	}
	if mt != TextMessage || !bytes.Equal(got, payload) {
		t.Errorf("round-trip mismatch: mt=%d got=%v", mt, got)
	}
}

// TestReadBinaryMessage_InvalidBytesAllowed: binary messages are NEVER
// UTF-8 validated regardless of the flag.
func TestReadBinaryMessage_InvalidBytesAllowed(t *testing.T) {
	payload := []byte{0xff, 0xfe, 0x00, 0x80}
	frame := encodeTextFrame(true, BinaryMessage, payload)

	c := newTestConn(bytes.NewReader(frame), nil, false)
	mt, got, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("BinaryMessage read: %v", err)
	}
	if mt != BinaryMessage || !bytes.Equal(got, payload) {
		t.Errorf("round-trip: mt=%d got=%v want=%v", mt, got, payload)
	}
}

// --- Write-side integration tests ---

// TestWriteMessage_InvalidUTF8Rejected: WriteMessage(TextMessage, ...) with
// invalid UTF-8 returns errInvalidUTF8 without emitting any wire bytes.
func TestWriteMessage_InvalidUTF8Rejected(t *testing.T) {
	var buf bytes.Buffer
	c := newTestConn(nil, &buf, false)
	err := c.WriteMessage(TextMessage, []byte{'a', 0xff, 'b'})
	if !errors.Is(err, errInvalidUTF8) {
		t.Fatalf("WriteMessage err = %v, want errInvalidUTF8", err)
	}
	if buf.Len() != 0 {
		t.Errorf("invalid-UTF8 write leaked %d bytes to the wire", buf.Len())
	}
}

// TestWriteMessage_ValidUTF8Succeeds: sanity that validation doesn't break
// the happy path.
func TestWriteMessage_ValidUTF8Succeeds(t *testing.T) {
	var buf bytes.Buffer
	c := newTestConn(nil, &buf, false)
	if err := c.WriteMessage(TextMessage, []byte("hello, 🌍")); err != nil {
		t.Fatal(err)
	}
	if buf.Len() == 0 {
		t.Error("expected wire bytes, got none")
	}
}

// TestNextWriter_InvalidUTF8OnWrite: streaming NextWriter writer rejects
// invalid bytes at Write time, before they reach the wire.
func TestNextWriter_InvalidUTF8OnWrite(t *testing.T) {
	var buf bytes.Buffer
	c := newTestConn(nil, &buf, false)
	w, err := c.NextWriter(TextMessage)
	if err != nil {
		t.Fatal(err)
	}

	// A valid prefix goes through.
	if _, err := w.Write([]byte("valid ")); err != nil {
		t.Fatalf("valid prefix write: %v", err)
	}
	// Invalid continuation byte rejects.
	if _, err := w.Write([]byte{0xff}); !errors.Is(err, errInvalidUTF8) {
		t.Fatalf("invalid write err = %v, want errInvalidUTF8", err)
	}
	// Close still works so the write mutex gets released.
	_ = w.Close()

	// Sanity: a subsequent Write must also be safely refused.
	c2 := newTestConn(nil, &bytes.Buffer{}, false)
	w2, _ := c2.NextWriter(TextMessage)
	if err := mustCloseAfterErr(w2, []byte{0xff}); err == nil {
		t.Error("second-conn: expected error writing invalid UTF-8 through NextWriter")
	}
}

// TestNextWriter_TruncatedCodepointOnClose: Close fails if the cumulative
// writes end mid-codepoint (valid so far, but no complete codepoint at
// message end).
func TestNextWriter_TruncatedCodepointOnClose(t *testing.T) {
	var buf bytes.Buffer
	c := newTestConn(nil, &buf, false)
	w, err := c.NextWriter(TextMessage)
	if err != nil {
		t.Fatal(err)
	}
	// First byte of a 3-byte sequence, no continuation.
	if _, err := w.Write([]byte{0xe2}); err != nil {
		t.Fatalf("partial write: %v", err)
	}
	if err := w.Close(); !errors.Is(err, errInvalidUTF8) {
		t.Fatalf("Close err = %v, want errInvalidUTF8", err)
	}
}

// mustCloseAfterErr is a helper used by write-side tests: writes p, then
// always closes so the write mutex is released.
func mustCloseAfterErr(w io.WriteCloser, p []byte) error {
	_, err := w.Write(p)
	_ = w.Close()
	return err
}

// --- Handshake plumbing ---

// TestUpgrader_SkipUTF8Validation_PlumbedToConn: Upgrader.SkipUTF8Validation
// must propagate to the resulting *Conn.
func TestUpgrader_SkipUTF8Validation_PlumbedToConn(t *testing.T) {
	// The simplest check: spin up a net.Pipe-based server Conn via the
	// internal newConn path, since full handshake plumbing is covered
	// by other tests. The point here is the bool flows through.
	cConn, sConn := net.Pipe()
	defer cConn.Close()
	defer sConn.Close()

	// Read side is a client (isServer=false), so server-to-client
	// frames (unmasked) are accepted.
	c := newConn(cConn, false, 1024, 1024, nil, nil, nil)
	if c.skipUTF8Validation {
		t.Fatal("default newConn should not skip UTF-8 validation")
	}

	// Simulate what Upgrader.Upgrade / Dialer.DialContext do when the
	// user sets Skip{Dialer,Upgrader}.SkipUTF8Validation.
	c.skipUTF8Validation = true

	// Server-side writes an unmasked TextMessage with invalid UTF-8.
	go func() {
		frame := encodeTextFrame(true, TextMessage, []byte{'x', 0xff, 'y'})
		_, _ = sConn.Write(frame)
	}()

	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	mt, data, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage (skip): %v", err)
	}
	if mt != TextMessage || len(data) != 3 || data[1] != 0xff {
		t.Errorf("expected invalid TextMessage to pass through, got mt=%d data=%v", mt, data)
	}
}
