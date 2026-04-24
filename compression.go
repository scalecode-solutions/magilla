// Copyright 2017 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package magilla

import (
	"compress/flate"
	"errors"
	"io"
	"strings"
	"sync"
)

const (
	minCompressionLevel     = -2 // flate.HuffmanOnly not defined in Go < 1.6
	maxCompressionLevel     = flate.BestCompression
	defaultCompressionLevel = 1

	// maxWindowBits is DEFLATE's LZ77 sliding-window size (32 KiB, 2^15).
	// Go's compress/flate hard-codes 15-bit windows; this constant is used
	// to clamp the userspace sliding-window buffer we maintain on the
	// receive side under context-takeover mode.
	maxWindowBits = 32768

	// deflateMessageTail is the 4-byte DEFLATE "empty non-final stored
	// block" marker that RFC 7692 §7.2.1 says senders MUST strip from the
	// end of each compressed message and receivers MUST reappend before
	// decoding. The following 5 bytes are a BFINAL=1 empty-stored-block
	// trailer injected on the receive side to make flate.Reader return
	// io.EOF cleanly — it's not part of the wire protocol, just a Go
	// stdlib workaround.
	deflateMessageTail     = "\x00\x00\xff\xff"
	deflateMessageTailRead = deflateMessageTail + "\x01\x00\x00\xff\xff"
)

// CompressionMode selects how permessage-deflate (RFC 7692) is configured.
// The zero value is CompressionModeDefault, which preserves backward
// compatibility by deriving the mode from the EnableCompression field:
// EnableCompression=true becomes CompressionModeNoContextTakeover,
// false becomes CompressionModeDisabled.
//
// Callers who want deterministic behavior should set CompressionMode
// explicitly and ignore EnableCompression.
type CompressionMode int

const (
	// CompressionModeDefault defers to the legacy EnableCompression field.
	CompressionModeDefault CompressionMode = iota

	// CompressionModeDisabled disables permessage-deflate negotiation
	// entirely, even when EnableCompression is true.
	CompressionModeDisabled

	// CompressionModeNoContextTakeover enables permessage-deflate with
	// the LZ77 dictionary reset between every message. Memory footprint
	// is minimal (per-message pooled allocation), compression ratio is
	// modest. This matches the default behavior of gorilla/websocket
	// when EnableCompression is true.
	CompressionModeNoContextTakeover

	// CompressionModeContextTakeover enables permessage-deflate with
	// the LZ77 dictionary PERSISTING across messages on a given
	// connection. For repetitive payloads (JSON deltas, telemetry,
	// chat) this dramatically improves compression ratio at the cost
	// of ~600 KiB to 1.2 MiB per connection of flate.Writer state.
	//
	// Reject any server_max_window_bits / client_max_window_bits offer
	// from the peer that isn't 15 — Go's compress/flate cannot honor
	// smaller windows. The handshake silently downgrades to
	// CompressionModeNoContextTakeover in that case.
	//
	// WritePreparedMessage is incompatible with context takeover and
	// will return ErrPreparedMessageContextTakeover. PreparedMessage
	// caches pre-compressed frames, but those frames are only decodable
	// against a matching dictionary history, which per-connection
	// takeover state does not satisfy.
	CompressionModeContextTakeover
)

// ErrPreparedMessageContextTakeover is returned by WritePreparedMessage
// when the target connection has context-takeover compression active.
// PreparedMessage caches a compressed frame computed against an empty
// dictionary, which is not decodable on a connection whose compression
// state is shared across messages.
var ErrPreparedMessageContextTakeover = errors.New("websocket: PreparedMessage is not compatible with context-takeover compression")

// pmdeflateParams captures the subset of permessage-deflate handshake
// parameters this package knows how to interpret.
type pmdeflateParams struct {
	serverNoContextTakeover bool
	clientNoContextTakeover bool
	// windowBitsValid is false when the peer offered or requested a
	// server_max_window_bits or client_max_window_bits value other than
	// the Go stdlib default (15). Callers treat an invalid windowBits
	// as grounds to decline the permessage-deflate offer entirely.
	windowBitsValid bool
}

// compressionOfferHeader returns the Sec-WebSocket-Extensions value a
// client should send for the given effective compression mode, or ""
// when the extension should not be offered at all.
//
// For CompressionModeNoContextTakeover we include both *_no_context_takeover
// params to preserve byte-for-byte backward compatibility with the
// pre-fork gorilla offer.
//
// For CompressionModeContextTakeover we offer bare "permessage-deflate"
// so the server is free to either grant takeover on both sides or force
// no-takeover on either side; the response parser handles the resulting
// four variants.
func compressionOfferHeader(mode CompressionMode) string {
	switch mode {
	case CompressionModeNoContextTakeover:
		return "permessage-deflate; server_no_context_takeover; client_no_context_takeover"
	case CompressionModeContextTakeover:
		return "permessage-deflate"
	default:
		return ""
	}
}

// parsePMDeflate walks the parameter map for a permessage-deflate
// extension entry and returns the parsed flags. The map is expected to
// come from parseExtensions (key "" is the extension name).
func parsePMDeflate(ext map[string]string) pmdeflateParams {
	p := pmdeflateParams{windowBitsValid: true}
	for k, v := range ext {
		switch k {
		case "":
			// extension name; skip
		case "server_no_context_takeover":
			p.serverNoContextTakeover = true
		case "client_no_context_takeover":
			p.clientNoContextTakeover = true
		case "server_max_window_bits":
			if v != "" && v != "15" {
				p.windowBitsValid = false
			}
		case "client_max_window_bits":
			// The bare form (no value) is legal in a client offer but
			// meaningless for us since we only support 15. Accept it
			// as "client said so but didn't specify"; a value of 15 is
			// also accepted. Anything else is declined.
			if v != "" && v != "15" {
				p.windowBitsValid = false
			}
		default:
			// Unknown parameters are ignored per RFC 7692 §5.1.
		}
	}
	return p
}

var (
	flateWriterPools [maxCompressionLevel - minCompressionLevel + 1]sync.Pool
	flateReaderPool  = sync.Pool{New: func() any {
		return flate.NewReader(nil)
	}}
)

func decompressNoContextTakeover(r io.Reader) io.ReadCloser {
	const tail =
	// Add four bytes as specified in RFC
	"\x00\x00\xff\xff" +
		// Add final block to squelch unexpected EOF error from flate reader.
		"\x01\x00\x00\xff\xff"

	fr, _ := flateReaderPool.Get().(io.ReadCloser)
	mr := io.MultiReader(r, strings.NewReader(tail))
	if err := fr.(flate.Resetter).Reset(mr, nil); err != nil {
		// Reset never fails, but handle error in case that changes.
		fr = flate.NewReader(mr)
	}
	return &flateReadWrapper{fr}
}

func isValidCompressionLevel(level int) bool {
	return minCompressionLevel <= level && level <= maxCompressionLevel
}

func compressNoContextTakeover(w io.WriteCloser, level int) io.WriteCloser {
	p := &flateWriterPools[level-minCompressionLevel]
	tw := &truncWriter{w: w}
	fw, _ := p.Get().(*flate.Writer)
	if fw == nil {
		fw, _ = flate.NewWriter(tw, level)
	} else {
		fw.Reset(tw)
	}
	return &flateWriteWrapper{fw: fw, tw: tw, p: p}
}

// truncWriter is an io.Writer that writes all but the last four bytes of the
// stream to another io.Writer.
type truncWriter struct {
	w io.WriteCloser
	n int
	p [4]byte
}

func (w *truncWriter) Write(p []byte) (int, error) {
	n := 0

	// fill buffer first for simplicity.
	if w.n < len(w.p) {
		n = copy(w.p[w.n:], p)
		p = p[n:]
		w.n += n
		if len(p) == 0 {
			return n, nil
		}
	}

	m := len(p)
	if m > len(w.p) {
		m = len(w.p)
	}

	if nn, err := w.w.Write(w.p[:m]); err != nil {
		return n + nn, err
	}

	copy(w.p[:], w.p[m:])
	copy(w.p[len(w.p)-m:], p[len(p)-m:])
	nn, err := w.w.Write(p[:len(p)-m])
	return n + nn, err
}

type flateWriteWrapper struct {
	fw *flate.Writer
	tw *truncWriter
	p  *sync.Pool
}

func (w *flateWriteWrapper) Write(p []byte) (int, error) {
	if w.fw == nil {
		return 0, errWriteClosed
	}
	return w.fw.Write(p)
}

func (w *flateWriteWrapper) Close() error {
	if w.fw == nil {
		return nil
	}
	err1 := w.fw.Flush()
	w.p.Put(w.fw)
	w.fw = nil
	if w.tw.p != [4]byte{0, 0, 0xff, 0xff} {
		return errors.New("websocket: internal error, unexpected bytes at end of flate stream")
	}
	err2 := w.tw.w.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

type flateReadWrapper struct {
	fr io.ReadCloser
}

func (r *flateReadWrapper) Read(p []byte) (int, error) {
	if r.fr == nil {
		return 0, io.ErrClosedPipe
	}
	n, err := r.fr.Read(p)
	if err == io.EOF {
		// Preemptively place the reader back in the pool. This helps with
		// scenarios where the application does not call NextReader() soon after
		// this final read.
		r.Close()
	}
	return n, err
}

func (r *flateReadWrapper) Close() error {
	if r.fr == nil {
		return nil
	}
	err := r.fr.Close()
	flateReaderPool.Put(r.fr)
	r.fr = nil
	return err
}

// --- Context-takeover ---
//
// The factories below implement RFC 7692 permessage-deflate with
// context takeover: the DEFLATE compressor / decompressor state persists
// across messages on a single connection, so LZ77 back-references from
// one message can reference bytes in a prior message.
//
// Pattern derived from the gorilla/websocket PR #342 by smith-30 and
// coder/websocket. Key implementation points:
//
//   - On the write side we keep a single flate.Writer for the connection's
//     entire lifetime. Flush() is called at message boundaries (emits
//     the 4-byte sync marker that the truncWriter strips); Close() is
//     only called when the connection itself closes, because Close
//     terminates the deflate stream and would invalidate the dictionary.
//
//   - On the read side Go's flate.Reader doesn't expose its internal
//     sliding window, so we maintain one in userspace. Every decompressed
//     byte is appended to a 32 KiB ring-capped buffer; at the start of the
//     next message we reset a fresh flate.Reader with that buffer as its
//     dictionary via flate.Resetter.Reset(r, dict).
//
//   - The RFC's trailing-4-bytes strip (truncWriter on write; mandatory
//     reappend on read) is orthogonal to takeover and remains unchanged.

// contextTakeoverWriterFactory owns the persistent flate.Writer for a
// connection configured with client-side (or server-side) context
// takeover. It is held by the Conn; each outgoing message calls
// newCompressionWriter to get a wrapper that retargets the truncWriter
// at the current write destination.
type contextTakeoverWriterFactory struct {
	fw *flate.Writer
	tw truncWriter
}

func (wf *contextTakeoverWriterFactory) newCompressionWriter(w io.WriteCloser, level int) io.WriteCloser {
	if wf.fw == nil {
		wf.fw, _ = flate.NewWriter(&wf.tw, level)
	}
	// Retarget the trunc writer at the new underlying destination and
	// reset its 4-byte tail buffer. The flate.Writer keeps its LZ77
	// dictionary and hash tables intact across this transition.
	wf.tw.w = w
	wf.tw.n = 0
	wf.tw.p = [4]byte{}
	return &flateTakeoverWriteWrapper{f: wf}
}

type flateTakeoverWriteWrapper struct {
	f *contextTakeoverWriterFactory
}

func (w *flateTakeoverWriteWrapper) Write(p []byte) (int, error) {
	if w.f == nil {
		return 0, errWriteClosed
	}
	return w.f.fw.Write(p)
}

func (w *flateTakeoverWriteWrapper) Close() error {
	if w.f == nil {
		return nil
	}
	f := w.f
	w.f = nil
	// Flush emits the sync marker 00 00 ff ff which truncWriter captures
	// in its 4-byte tail. Do NOT call f.fw.Close(): that terminates the
	// DEFLATE stream and invalidates the dictionary we're trying to
	// preserve.
	err1 := f.fw.Flush()
	if f.tw.p != [4]byte{0, 0, 0xff, 0xff} {
		return errors.New("websocket: internal error, unexpected bytes at end of flate stream")
	}
	err2 := f.tw.w.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

// contextTakeoverReaderFactory owns the userspace sliding window used
// to seed flate.Reader state across messages under context takeover.
// Unlike the write side, Go's compress/flate doesn't let us preserve
// decoder state across resets, so we capture every decompressed byte
// into `window` and re-seed it via flate.Resetter.Reset(r, window) on
// the next message.
type contextTakeoverReaderFactory struct {
	fr     io.ReadCloser
	window []byte
}

func (f *contextTakeoverReaderFactory) newDecompressionReader(r io.Reader) io.ReadCloser {
	if f.fr == nil {
		f.fr = flate.NewReader(nil)
	}
	mr := io.MultiReader(r, strings.NewReader(deflateMessageTailRead))
	// Reset with our captured window as the pre-populated LZ77
	// dictionary. Reset never fails on the standard flate.Reader, but
	// handle the error in case of a future behavior change.
	if err := f.fr.(flate.Resetter).Reset(mr, f.window); err != nil {
		f.fr = flate.NewReader(mr)
	}
	return &flateTakeoverReadWrapper{f: f}
}

type flateTakeoverReadWrapper struct {
	f *contextTakeoverReaderFactory
}

func (r *flateTakeoverReadWrapper) Read(p []byte) (int, error) {
	if r.f == nil || r.f.fr == nil {
		return 0, io.ErrClosedPipe
	}
	n, err := r.f.fr.Read(p)
	if n > 0 {
		// Capture decompressed bytes into the sliding window so the
		// NEXT message's reader can back-reference them.
		r.f.window = append(r.f.window, p[:n]...)
		if len(r.f.window) > maxWindowBits {
			r.f.window = r.f.window[len(r.f.window)-maxWindowBits:]
		}
	}
	return n, err
}

func (r *flateTakeoverReadWrapper) Close() error {
	if r.f == nil {
		return nil
	}
	r.f = nil
	// Do NOT call r.f.fr.Close(); the underlying reader is reused on
	// the next message. Closing it prematurely and then Reset'ing is
	// technically fine today but relies on implementation behavior.
	return nil
}
