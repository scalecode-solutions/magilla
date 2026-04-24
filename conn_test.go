// Copyright 2013 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package magilla

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"testing"
	"testing/iotest"
	"time"
)

var _ net.Error = errWriteTimeout

type fakeNetConn struct {
	io.Reader
	io.Writer
}

func (c fakeNetConn) Close() error                       { return nil }
func (c fakeNetConn) LocalAddr() net.Addr                { return localAddr }
func (c fakeNetConn) RemoteAddr() net.Addr               { return remoteAddr }
func (c fakeNetConn) SetDeadline(t time.Time) error      { return nil }
func (c fakeNetConn) SetReadDeadline(t time.Time) error  { return nil }
func (c fakeNetConn) SetWriteDeadline(t time.Time) error { return nil }

type fakeAddr int

var (
	localAddr  = fakeAddr(1)
	remoteAddr = fakeAddr(2)
)

func (a fakeAddr) Network() string {
	return "net"
}

func (a fakeAddr) String() string {
	return "str"
}

// newTestConn creates a connection backed by a fake network connection using
// default values for buffering.
func newTestConn(r io.Reader, w io.Writer, isServer bool) *Conn {
	return newConn(fakeNetConn{Reader: r, Writer: w}, isServer, 1024, 1024, nil, nil, nil)
}

func TestFraming(t *testing.T) {
	frameSizes := []int{
		0, 1, 2, 124, 125, 126, 127, 128, 129, 65534, 65535,
		// 65536, 65537
	}
	var readChunkers = []struct {
		name string
		f    func(io.Reader) io.Reader
	}{
		{"half", iotest.HalfReader},
		{"one", iotest.OneByteReader},
		{"asis", func(r io.Reader) io.Reader { return r }},
	}
	writeBuf := make([]byte, 65537)
	for i := range writeBuf {
		writeBuf[i] = byte(i)
	}
	var writers = []struct {
		name string
		f    func(w io.Writer, n int) (int, error)
	}{
		{"iocopy", func(w io.Writer, n int) (int, error) {
			nn, err := io.Copy(w, bytes.NewReader(writeBuf[:n]))
			return int(nn), err
		}},
		{"write", func(w io.Writer, n int) (int, error) {
			return w.Write(writeBuf[:n])
		}},
		{"string", func(w io.Writer, n int) (int, error) {
			return io.WriteString(w, string(writeBuf[:n]))
		}},
	}

	for _, compress := range []bool{false, true} {
		for _, isServer := range []bool{true, false} {
			for _, chunker := range readChunkers {

				var connBuf bytes.Buffer
				wc := newTestConn(nil, &connBuf, isServer)
				rc := newTestConn(chunker.f(&connBuf), nil, !isServer)
				if compress {
					wc.newCompressionWriter = compressNoContextTakeover
					rc.newDecompressionReader = decompressNoContextTakeover
				}
				for _, n := range frameSizes {
					for _, writer := range writers {
						name := fmt.Sprintf("z:%v, s:%v, r:%s, n:%d w:%s", compress, isServer, chunker.name, n, writer.name)

						w, err := wc.NextWriter(TextMessage)
						if err != nil {
							t.Errorf("%s: wc.NextWriter() returned %v", name, err)
							continue
						}
						nn, err := writer.f(w, n)
						if err != nil || nn != n {
							t.Errorf("%s: w.Write(writeBuf[:n]) returned %d, %v", name, nn, err)
							continue
						}
						err = w.Close()
						if err != nil {
							t.Errorf("%s: w.Close() returned %v", name, err)
							continue
						}

						opCode, r, err := rc.NextReader()
						if err != nil || opCode != TextMessage {
							t.Errorf("%s: NextReader() returned %d, r, %v", name, opCode, err)
							continue
						}

						t.Logf("frame size: %d", n)
						rbuf, err := io.ReadAll(r)
						if err != nil {
							t.Errorf("%s: ReadFull() returned rbuf, %v", name, err)
							continue
						}

						if len(rbuf) != n {
							t.Errorf("%s: len(rbuf) is %d, want %d", name, len(rbuf), n)
							continue
						}

						for i, b := range rbuf {
							if byte(i) != b {
								t.Errorf("%s: bad byte at offset %d", name, i)
								break
							}
						}
					}
				}
			}
		}
	}
}

func TestWriteControlDeadline(t *testing.T) {
	t.Parallel()
	message := []byte("hello")
	var connBuf bytes.Buffer
	c := newTestConn(nil, &connBuf, true)
	if err := c.WriteControl(PongMessage, message, time.Time{}); err != nil {
		t.Errorf("WriteControl(..., zero deadline) = %v, want nil", err)
	}
	if err := c.WriteControl(PongMessage, message, time.Now().Add(time.Second)); err != nil {
		t.Errorf("WriteControl(..., future deadline) = %v, want nil", err)
	}
	if err := c.WriteControl(PongMessage, message, time.Now().Add(-time.Second)); err == nil {
		t.Errorf("WriteControl(..., past deadline) = nil, want timeout error")
	}
}

func TestConcurrencyWriteControl(t *testing.T) {
	const message = "this is a ping/pong messsage"
	loop := 10
	workers := 10
	for i := 0; i < loop; i++ {
		var connBuf bytes.Buffer

		wg := sync.WaitGroup{}
		wc := newTestConn(nil, &connBuf, true)

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := wc.WriteControl(PongMessage, []byte(message), time.Now().Add(time.Second)); err != nil {
					t.Errorf("concurrently wc.WriteControl() returned %v", err)
				}
			}()
		}

		wg.Wait()
		wc.Close()
	}
}

// TestConcurrencyWriteMessage exercises the concurrent-safe-writes contract:
// N goroutines calling WriteMessage concurrently are serialized by the
// outer writeMu, each message lands on the wire atomically (no frame
// interleave), and all messages are delivered.
func TestConcurrencyWriteMessage(t *testing.T) {
	const workers = 32
	const payload = "payload-payload-payload-payload-" // 32 bytes, non-fragmenting

	var connBuf bytes.Buffer
	wc := newTestConn(nil, &connBuf, true)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := wc.WriteMessage(TextMessage, []byte(payload)); err != nil {
				t.Errorf("concurrent WriteMessage: %v", err)
			}
		}()
	}
	wg.Wait()

	// Read them back off the wire. Server-side framing: opcode+len then
	// payload, server-to-client has no mask. Each message is a single
	// frame (payload < 126 bytes).
	rc := newTestConn(&connBuf, nil, false)
	seen := 0
	for {
		mt, data, err := rc.ReadMessage()
		if err != nil {
			break
		}
		if mt != TextMessage {
			t.Fatalf("message %d: got type %d, want TextMessage", seen, mt)
		}
		if string(data) != payload {
			t.Fatalf("message %d: payload corrupted (len %d, starts %q)", seen, len(data), string(data[:min(10, len(data))]))
		}
		seen++
	}
	if seen != workers {
		t.Errorf("got %d messages, want %d", seen, workers)
	}
}

// TestConcurrencyNextWriter verifies that multiple goroutines each using
// NextWriter...Close are serialized and that the streaming writes of one
// goroutine don't interleave frames with another's.
func TestConcurrencyNextWriter(t *testing.T) {
	const workers = 16
	// Intentionally choose a payload size that fragments: larger than the
	// default write buffer forces multiple flushFrame calls per message.
	payload := bytes.Repeat([]byte("AB"), 8192)

	var connBuf bytes.Buffer
	wc := newTestConn(nil, &connBuf, true)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, err := wc.NextWriter(BinaryMessage)
			if err != nil {
				t.Errorf("NextWriter: %v", err)
				return
			}
			// Two Writes so flushFrame fires at least once mid-stream.
			if _, err := w.Write(payload[:len(payload)/2]); err != nil {
				t.Errorf("write 1: %v", err)
				_ = w.Close()
				return
			}
			if _, err := w.Write(payload[len(payload)/2:]); err != nil {
				t.Errorf("write 2: %v", err)
				_ = w.Close()
				return
			}
			if err := w.Close(); err != nil {
				t.Errorf("close: %v", err)
			}
		}()
	}
	wg.Wait()

	rc := newTestConn(&connBuf, nil, false)
	seen := 0
	for {
		mt, data, err := rc.ReadMessage()
		if err != nil {
			break
		}
		if mt != BinaryMessage {
			t.Fatalf("message %d: got type %d, want BinaryMessage", seen, mt)
		}
		if !bytes.Equal(data, payload) {
			t.Fatalf("message %d: payload corrupted (len %d, want %d)", seen, len(data), len(payload))
		}
		seen++
	}
	if seen != workers {
		t.Errorf("got %d messages, want %d", seen, workers)
	}
}

// TestWriteMessageDeadlineDuringAcquire verifies #704: a caller whose
// writeDeadline fires while another goroutine holds the write mutex
// returns errWriteTimeout promptly, rather than blocking past its deadline.
func TestWriteMessageDeadlineDuringAcquire(t *testing.T) {
	bw := newBlockingWriter()
	c := newTestConn(nil, bw, false)

	// Goroutine 1: grabs writeMu and stalls inside the underlying Write.
	go func() {
		_ = c.WriteMessage(TextMessage, []byte("first"))
	}()
	<-bw.c1 // first goroutine is now blocked in Write while holding writeMu

	// Goroutine 2: write deadline 100ms. With writeMu held indefinitely,
	// acquireWriteMu must return errWriteTimeout near the deadline.
	if err := c.SetWriteDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err := c.WriteMessage(TextMessage, []byte("second"))
	elapsed := time.Since(start)

	if err != errWriteTimeout {
		t.Fatalf("got %v, want errWriteTimeout", err)
	}
	// Tolerate a 100ms slack for slow CI boxes.
	if elapsed > 300*time.Millisecond {
		t.Errorf("deadline took too long: %v", elapsed)
	}

	// Release the first goroutine so the test finishes cleanly.
	close(bw.c2)
}

func TestControl(t *testing.T) {
	const message = "this is a ping/pong message"
	for _, isServer := range []bool{true, false} {
		for _, isWriteControl := range []bool{true, false} {
			name := fmt.Sprintf("s:%v, wc:%v", isServer, isWriteControl)
			var connBuf bytes.Buffer
			wc := newTestConn(nil, &connBuf, isServer)
			rc := newTestConn(&connBuf, nil, !isServer)
			if isWriteControl {
				_ = wc.WriteControl(PongMessage, []byte(message), time.Now().Add(time.Second))
			} else {
				w, err := wc.NextWriter(PongMessage)
				if err != nil {
					t.Errorf("%s: wc.NextWriter() returned %v", name, err)
					continue
				}
				if _, err := w.Write([]byte(message)); err != nil {
					t.Errorf("%s: w.Write() returned %v", name, err)
					continue
				}
				if err := w.Close(); err != nil {
					t.Errorf("%s: w.Close() returned %v", name, err)
					continue
				}
				var actualMessage string
				rc.SetPongHandler(func(s string) error { actualMessage = s; return nil })
				_, _, _ = rc.NextReader()
				if actualMessage != message {
					t.Errorf("%s: pong=%q, want %q", name, actualMessage, message)
					continue
				}
			}
		}
	}
}

// simpleBufferPool is an implementation of BufferPool for TestWriteBufferPool.
type simpleBufferPool struct {
	v any
}

func (p *simpleBufferPool) Get() any {
	v := p.v
	p.v = nil
	return v
}

func (p *simpleBufferPool) Put(v any) {
	p.v = v
}

func TestWriteBufferPool(t *testing.T) {
	const message = "Now is the time for all good people to come to the aid of the party."

	var buf bytes.Buffer
	var pool simpleBufferPool
	rc := newTestConn(&buf, nil, false)

	// Specify writeBufferSize smaller than message size to ensure that pooling
	// works with fragmented messages.
	wc := newConn(fakeNetConn{Writer: &buf}, true, 1024, len(message)-1, &pool, nil, nil)

	if wc.writeBuf != nil {
		t.Fatal("writeBuf not nil after create")
	}

	// Part 1: test NextWriter/Write/Close

	w, err := wc.NextWriter(TextMessage)
	if err != nil {
		t.Fatalf("wc.NextWriter() returned %v", err)
	}

	if wc.writeBuf == nil {
		t.Fatal("writeBuf is nil after NextWriter")
	}

	writeBufAddr := &wc.writeBuf[0]

	if _, err := io.WriteString(w, message); err != nil {
		t.Fatalf("io.WriteString(w, message) returned %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("w.Close() returned %v", err)
	}

	if wc.writeBuf != nil {
		t.Fatal("writeBuf not nil after w.Close()")
	}

	if wpd, ok := pool.v.(writePoolData); !ok || len(wpd.buf) == 0 || &wpd.buf[0] != writeBufAddr {
		t.Fatal("writeBuf not returned to pool")
	}

	opCode, p, err := rc.ReadMessage()
	if opCode != TextMessage || err != nil {
		t.Fatalf("ReadMessage() returned %d, p, %v", opCode, err)
	}

	if s := string(p); s != message {
		t.Fatalf("message is %s, want %s", s, message)
	}

	// Part 2: Test WriteMessage.

	if err := wc.WriteMessage(TextMessage, []byte(message)); err != nil {
		t.Fatalf("wc.WriteMessage() returned %v", err)
	}

	if wc.writeBuf != nil {
		t.Fatal("writeBuf not nil after wc.WriteMessage()")
	}

	if wpd, ok := pool.v.(writePoolData); !ok || len(wpd.buf) == 0 || &wpd.buf[0] != writeBufAddr {
		t.Fatal("writeBuf not returned to pool after WriteMessage")
	}

	opCode, p, err = rc.ReadMessage()
	if opCode != TextMessage || err != nil {
		t.Fatalf("ReadMessage() returned %d, p, %v", opCode, err)
	}

	if s := string(p); s != message {
		t.Fatalf("message is %s, want %s", s, message)
	}
}

// TestWriteBufferPoolSync ensures that *sync.Pool works as a buffer pool.
func TestWriteBufferPoolSync(t *testing.T) {
	var buf bytes.Buffer
	var pool sync.Pool
	wc := newConn(fakeNetConn{Writer: &buf}, true, 1024, 1024, &pool, nil, nil)
	rc := newTestConn(&buf, nil, false)

	const message = "Hello World!"
	for i := 0; i < 3; i++ {
		if err := wc.WriteMessage(TextMessage, []byte(message)); err != nil {
			t.Fatalf("wc.WriteMessage() returned %v", err)
		}
		opCode, p, err := rc.ReadMessage()
		if opCode != TextMessage || err != nil {
			t.Fatalf("ReadMessage() returned %d, p, %v", opCode, err)
		}
		if s := string(p); s != message {
			t.Fatalf("message is %s, want %s", s, message)
		}
	}
}

// errorWriter is an io.Writer than returns an error on all writes.
type errorWriter struct{}

func (ew errorWriter) Write(p []byte) (int, error) { return 0, errors.New("error") }

// TestWriteBufferPoolError ensures that buffer is returned to pool after error
// on write.
func TestWriteBufferPoolError(t *testing.T) {

	// Part 1: Test NextWriter/Write/Close

	var pool simpleBufferPool
	wc := newConn(fakeNetConn{Writer: errorWriter{}}, true, 1024, 1024, &pool, nil, nil)

	w, err := wc.NextWriter(TextMessage)
	if err != nil {
		t.Fatalf("wc.NextWriter() returned %v", err)
	}

	if wc.writeBuf == nil {
		t.Fatal("writeBuf is nil after NextWriter")
	}

	writeBufAddr := &wc.writeBuf[0]

	if _, err := io.WriteString(w, "Hello"); err != nil {
		t.Fatalf("io.WriteString(w, message) returned %v", err)
	}

	if err := w.Close(); err == nil {
		t.Fatalf("w.Close() did not return error")
	}

	if wpd, ok := pool.v.(writePoolData); !ok || len(wpd.buf) == 0 || &wpd.buf[0] != writeBufAddr {
		t.Fatal("writeBuf not returned to pool")
	}

	// Part 2: Test WriteMessage

	wc = newConn(fakeNetConn{Writer: errorWriter{}}, true, 1024, 1024, &pool, nil, nil)

	if err := wc.WriteMessage(TextMessage, []byte("Hello")); err == nil {
		t.Fatalf("wc.WriteMessage did not return error")
	}

	if wpd, ok := pool.v.(writePoolData); !ok || len(wpd.buf) == 0 || &wpd.buf[0] != writeBufAddr {
		t.Fatal("writeBuf not returned to pool")
	}
}

func TestCloseFrameBeforeFinalMessageFrame(t *testing.T) {
	const bufSize = 512

	expectedErr := &CloseError{Code: CloseNormalClosure, Text: "hello"}

	var b1, b2 bytes.Buffer
	wc := newConn(&fakeNetConn{Reader: nil, Writer: &b1}, false, 1024, bufSize, nil, nil, nil)
	rc := newTestConn(&b1, &b2, true)

	w, _ := wc.NextWriter(BinaryMessage)
	_, _ = w.Write(make([]byte, bufSize+bufSize/2))
	_ = wc.WriteControl(CloseMessage, FormatCloseMessage(expectedErr.Code, expectedErr.Text), time.Now().Add(10*time.Second))
	w.Close()

	op, r, err := rc.NextReader()
	if op != BinaryMessage || err != nil {
		t.Fatalf("NextReader() returned %d, %v", op, err)
	}
	_, err = io.Copy(io.Discard, r)
	if !reflect.DeepEqual(err, expectedErr) {
		t.Fatalf("io.Copy() returned %v, want %v", err, expectedErr)
	}
	_, _, err = rc.NextReader()
	if !reflect.DeepEqual(err, expectedErr) {
		t.Fatalf("NextReader() returned %v, want %v", err, expectedErr)
	}
}

func TestEOFWithinFrame(t *testing.T) {
	const bufSize = 64

	for n := 0; ; n++ {
		var b bytes.Buffer
		wc := newTestConn(nil, &b, false)
		rc := newTestConn(&b, nil, true)

		w, _ := wc.NextWriter(BinaryMessage)
		_, _ = w.Write(make([]byte, bufSize))
		w.Close()

		if n >= b.Len() {
			break
		}
		b.Truncate(n)

		op, r, err := rc.NextReader()
		if err == errUnexpectedEOF {
			continue
		}
		if op != BinaryMessage || err != nil {
			t.Fatalf("%d: NextReader() returned %d, %v", n, op, err)
		}
		_, err = io.Copy(io.Discard, r)
		if err != errUnexpectedEOF {
			t.Fatalf("%d: io.Copy() returned %v, want %v", n, err, errUnexpectedEOF)
		}
		_, _, err = rc.NextReader()
		if err != errUnexpectedEOF {
			t.Fatalf("%d: NextReader() returned %v, want %v", n, err, errUnexpectedEOF)
		}
	}
}

func TestEOFBeforeFinalFrame(t *testing.T) {
	const bufSize = 512

	var b1, b2 bytes.Buffer
	wc := newConn(&fakeNetConn{Writer: &b1}, false, 1024, bufSize, nil, nil, nil)
	rc := newTestConn(&b1, &b2, true)

	w, _ := wc.NextWriter(BinaryMessage)
	_, _ = w.Write(make([]byte, bufSize+bufSize/2))

	op, r, err := rc.NextReader()
	if op != BinaryMessage || err != nil {
		t.Fatalf("NextReader() returned %d, %v", op, err)
	}
	_, err = io.Copy(io.Discard, r)
	if err != errUnexpectedEOF {
		t.Fatalf("io.Copy() returned %v, want %v", err, errUnexpectedEOF)
	}
	_, _, err = rc.NextReader()
	if err != errUnexpectedEOF {
		t.Fatalf("NextReader() returned %v, want %v", err, errUnexpectedEOF)
	}
}

func TestWriteAfterMessageWriterClose(t *testing.T) {
	wc := newTestConn(nil, &bytes.Buffer{}, false)
	w, _ := wc.NextWriter(BinaryMessage)
	_, _ = io.WriteString(w, "hello")
	if err := w.Close(); err != nil {
		t.Fatalf("unexpected error closing message writer, %v", err)
	}

	if _, err := io.WriteString(w, "world"); err == nil {
		t.Fatalf("no error writing after close")
	}

	// Concurrent-safe-writes contract change: NextWriter now holds the
	// connection's write mutex until messageWriter.Close is called. The
	// legacy "call NextWriter again to implicitly close the previous
	// writer" shortcut has been retired - call Close explicitly. Writes
	// to the first writer after Close still return an error.
	w, _ = wc.NextWriter(BinaryMessage)
	_, _ = io.WriteString(w, "hello")
	if err := w.Close(); err != nil {
		t.Fatalf("unexpected error closing message writer, %v", err)
	}

	w2, err := wc.NextWriter(BinaryMessage)
	if err != nil {
		t.Fatalf("unexpected error getting next writer, %v", err)
	}
	if _, err := io.WriteString(w, "world"); err == nil {
		t.Fatalf("no error writing after close")
	}
	_ = w2.Close()
}

func TestReadLimit(t *testing.T) {
	t.Run("Test ReadLimit is enforced", func(t *testing.T) {
		const readLimit = 512
		message := make([]byte, readLimit+1)

		var b1, b2 bytes.Buffer
		wc := newConn(&fakeNetConn{Writer: &b1}, false, 1024, readLimit-2, nil, nil, nil)
		rc := newTestConn(&b1, &b2, true)
		rc.SetReadLimit(readLimit)

		// Send message at the limit with interleaved pong.
		w, _ := wc.NextWriter(BinaryMessage)
		_, _ = w.Write(message[:readLimit-1])
		_ = wc.WriteControl(PongMessage, []byte("this is a pong"), time.Now().Add(10*time.Second))
		_, _ = w.Write(message[:1])
		w.Close()

		// Send message larger than the limit.
		_ = wc.WriteMessage(BinaryMessage, message[:readLimit+1])

		op, _, err := rc.NextReader()
		if op != BinaryMessage || err != nil {
			t.Fatalf("1: NextReader() returned %d, %v", op, err)
		}
		op, r, err := rc.NextReader()
		if op != BinaryMessage || err != nil {
			t.Fatalf("2: NextReader() returned %d, %v", op, err)
		}
		_, err = io.Copy(io.Discard, r)
		if err != ErrReadLimit {
			t.Fatalf("io.Copy() returned %v", err)
		}
	})

	t.Run("Test that ReadLimit cannot be overflowed", func(t *testing.T) {
		const readLimit = 1

		var b1, b2 bytes.Buffer
		rc := newTestConn(&b1, &b2, true)
		rc.SetReadLimit(readLimit)

		// First, send a non-final binary message
		b1.Write([]byte("\x02\x81"))

		// Mask key
		b1.Write([]byte("\x00\x00\x00\x00"))

		// First payload
		b1.Write([]byte("A"))

		// Next, send a negative-length, non-final continuation frame
		b1.Write([]byte("\x00\xFF\x80\x00\x00\x00\x00\x00\x00\x00"))

		// Mask key
		b1.Write([]byte("\x00\x00\x00\x00"))

		// Next, send a too long, final continuation frame
		b1.Write([]byte("\x80\xFF\x00\x00\x00\x00\x00\x00\x00\x05"))

		// Mask key
		b1.Write([]byte("\x00\x00\x00\x00"))

		// Too-long payload
		b1.Write([]byte("BCDEF"))

		op, r, err := rc.NextReader()
		if op != BinaryMessage || err != nil {
			t.Fatalf("1: NextReader() returned %d, %v", op, err)
		}

		var buf [10]byte
		var read int
		n, err := r.Read(buf[:])
		if err != nil && err != ErrReadLimit {
			t.Fatalf("unexpected error testing read limit: %v", err)
		}
		read += n

		n, err = r.Read(buf[:])
		if err != nil && err != ErrReadLimit {
			t.Fatalf("unexpected error testing read limit: %v", err)
		}
		read += n

		if err == nil && read > readLimit {
			t.Fatalf("read limit exceeded: limit %d, read %d", readLimit, read)
		}
	})
}

func TestAddrs(t *testing.T) {
	c := newTestConn(nil, nil, true)
	if c.LocalAddr() != localAddr {
		t.Errorf("LocalAddr = %v, want %v", c.LocalAddr(), localAddr)
	}
	if c.RemoteAddr() != remoteAddr {
		t.Errorf("RemoteAddr = %v, want %v", c.RemoteAddr(), remoteAddr)
	}
}

func TestDeprecatedUnderlyingConn(t *testing.T) {
	var b1, b2 bytes.Buffer
	fc := fakeNetConn{Reader: &b1, Writer: &b2}
	c := newConn(fc, true, 1024, 1024, nil, nil, nil)
	ul := c.UnderlyingConn()
	if ul != fc {
		t.Fatalf("Underlying conn is not what it should be.")
	}
}

func TestNetConn(t *testing.T) {
	var b1, b2 bytes.Buffer
	fc := fakeNetConn{Reader: &b1, Writer: &b2}
	c := newConn(fc, true, 1024, 1024, nil, nil, nil)
	ul := c.NetConn()
	if ul != fc {
		t.Fatalf("Underlying conn is not what it should be.")
	}
}

func TestBufioReadBytes(t *testing.T) {
	// Test calling bufio.ReadBytes for value longer than read buffer size.

	m := make([]byte, 512)
	m[len(m)-1] = '\n'

	var b1, b2 bytes.Buffer
	wc := newConn(fakeNetConn{Writer: &b1}, false, len(m)+64, len(m)+64, nil, nil, nil)
	rc := newConn(fakeNetConn{Reader: &b1, Writer: &b2}, true, len(m)-64, len(m)-64, nil, nil, nil)

	w, _ := wc.NextWriter(BinaryMessage)
	_, _ = w.Write(m)
	w.Close()

	op, r, err := rc.NextReader()
	if op != BinaryMessage || err != nil {
		t.Fatalf("NextReader() returned %d, %v", op, err)
	}

	br := bufio.NewReader(r)
	p, err := br.ReadBytes('\n')
	if err != nil {
		t.Fatalf("ReadBytes() returned %v", err)
	}
	if len(p) != len(m) {
		t.Fatalf("read returned %d bytes, want %d bytes", len(p), len(m))
	}
}

var closeErrorTests = []struct {
	err   error
	codes []int
	ok    bool
}{
	{&CloseError{Code: CloseNormalClosure}, []int{CloseNormalClosure}, true},
	{&CloseError{Code: CloseNormalClosure}, []int{CloseNoStatusReceived}, false},
	{&CloseError{Code: CloseNormalClosure}, []int{CloseNoStatusReceived, CloseNormalClosure}, true},
	{errors.New("hello"), []int{CloseNormalClosure}, false},
}

func TestCloseError(t *testing.T) {
	for _, tt := range closeErrorTests {
		ok := IsCloseError(tt.err, tt.codes...)
		if ok != tt.ok {
			t.Errorf("IsCloseError(%#v, %#v) returned %v, want %v", tt.err, tt.codes, ok, tt.ok)
		}
	}
}

var unexpectedCloseErrorTests = []struct {
	err   error
	codes []int
	ok    bool
}{
	{&CloseError{Code: CloseNormalClosure}, []int{CloseNormalClosure}, false},
	{&CloseError{Code: CloseNormalClosure}, []int{CloseNoStatusReceived}, true},
	{&CloseError{Code: CloseNormalClosure}, []int{CloseNoStatusReceived, CloseNormalClosure}, false},
	{errors.New("hello"), []int{CloseNormalClosure}, false},
}

func TestUnexpectedCloseErrors(t *testing.T) {
	for _, tt := range unexpectedCloseErrorTests {
		ok := IsUnexpectedCloseError(tt.err, tt.codes...)
		if ok != tt.ok {
			t.Errorf("IsUnexpectedCloseError(%#v, %#v) returned %v, want %v", tt.err, tt.codes, ok, tt.ok)
		}
	}
}

type blockingWriter struct {
	c1, c2 chan struct{}
	once   *sync.Once
}

func newBlockingWriter() *blockingWriter {
	return &blockingWriter{
		c1:   make(chan struct{}),
		c2:   make(chan struct{}),
		once: &sync.Once{},
	}
}

// Write blocks on the first call until the test releases c2; subsequent
// calls pass through without blocking. This lets a test suspend the first
// writer mid-Write to observe concurrent callers queueing on writeMu.
func (w *blockingWriter) Write(p []byte) (int, error) {
	first := false
	w.once.Do(func() {
		first = true
	})
	if first {
		close(w.c1)
		<-w.c2
	}
	return len(p), nil
}

// TestConcurrentWriteSerialized replaces the former TestConcurrentWritePanic.
// Under the new contract, concurrent WriteMessage calls are safely
// serialized instead of panicking: a second caller blocks in acquireWriteMu
// until the first finishes. This test verifies that blocking-then-unblock
// sequence rather than asserting a panic.
func TestConcurrentWriteSerialized(t *testing.T) {
	w := newBlockingWriter()
	c := newTestConn(nil, w, false)

	firstDone := make(chan struct{})
	go func() {
		_ = c.WriteMessage(TextMessage, []byte{})
		close(firstDone)
	}()

	// Wait for the first goroutine to block in the underlying Write (it
	// holds writeMu at this point).
	<-w.c1

	// The second WriteMessage must block on acquireWriteMu rather than
	// panic. Confirm by racing it against a short timer.
	secondDone := make(chan struct{})
	go func() {
		_ = c.WriteMessage(TextMessage, []byte{})
		close(secondDone)
	}()
	select {
	case <-secondDone:
		t.Fatal("second WriteMessage should have blocked while first holds writeMu")
	case <-time.After(50 * time.Millisecond):
		// expected: still blocked
	}

	// Unblock the first. Both should complete cleanly.
	close(w.c2)
	<-firstDone
	<-secondDone
}

type failingReader struct{}

func (r failingReader) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func TestFailedConnectionReadPanic(t *testing.T) {
	c := newTestConn(failingReader{}, nil, false)

	defer func() {
		if v := recover(); v != nil {
			return
		}
	}()

	for i := 0; i < 20000; i++ {
		_, _, _ = c.ReadMessage()
	}
	t.Fatal("should not get here")
}

// TestDisableClientMask verifies that a client with disableMask=true
// writes frames without the MASK bit set and without a mask key. A
// conformant client always masks; this test just confirms the opt-out
// path works for peers that have explicitly agreed to skip masking.
func TestDisableClientMask(t *testing.T) {
	var connBuf bytes.Buffer
	c := newTestConn(nil, &connBuf, false) // isServer=false -> client
	c.disableMask = true

	payload := []byte("hello-unmasked")
	if err := c.WriteMessage(TextMessage, payload); err != nil {
		t.Fatal(err)
	}

	data := connBuf.Bytes()
	if len(data) < 2 {
		t.Fatalf("frame too short: %d bytes", len(data))
	}
	b1 := data[1]
	if b1&0x80 != 0 {
		t.Errorf("MASK bit set on disableMask=true client frame: b1=0x%02x", b1)
	}
	payloadLen := int(b1 & 0x7f)
	if payloadLen != len(payload) {
		t.Fatalf("unexpected length encoding: got %d, want %d", payloadLen, len(payload))
	}
	// With no mask key, payload starts at byte 2.
	if !bytes.Equal(data[2:2+len(payload)], payload) {
		t.Errorf("payload not plaintext: got %q", data[2:2+len(payload)])
	}

	// Control path: verify WriteControl also respects the flag.
	connBuf.Reset()
	if err := c.WriteControl(PingMessage, []byte("p"), time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	data = connBuf.Bytes()
	if data[1]&0x80 != 0 {
		t.Errorf("MASK bit set on disableMask=true control frame: %#v", data)
	}
}

// TestSetWriteFrameSize verifies that an outbound message larger than the
// configured per-frame cap is split into multiple frames on the wire and
// still round-trips correctly on the receiver side.
func TestSetWriteFrameSize(t *testing.T) {
	tests := []struct {
		name       string
		isServer   bool
		method     string // "write" | "readfrom" | "message"
		frameCap   int
		payloadLen int
		wantFrames int // minimum frames expected on wire
	}{
		{"server Write fragments", true, "write", 100, 550, 6},
		{"server ReadFrom fragments", true, "readfrom", 100, 550, 6},
		{"server WriteMessage fragments", true, "message", 100, 550, 6},
		{"client Write fragments", false, "write", 100, 550, 6},
		// No cap: a small message is a single frame.
		{"no cap single frame", true, "write", 0, 200, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var connBuf bytes.Buffer
			wc := newTestConn(nil, &connBuf, tt.isServer)
			wc.SetWriteFrameSize(tt.frameCap)

			payload := make([]byte, tt.payloadLen)
			for i := range payload {
				payload[i] = byte(i)
			}

			switch tt.method {
			case "write":
				w, err := wc.NextWriter(BinaryMessage)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := w.Write(payload); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
			case "readfrom":
				w, err := wc.NextWriter(BinaryMessage)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := io.Copy(w, bytes.NewReader(payload)); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
			case "message":
				if err := wc.WriteMessage(BinaryMessage, payload); err != nil {
					t.Fatal(err)
				}
			}

			// Count frames on the wire by parsing frame headers. We
			// don't need to fully parse; just walk the length prefix.
			data := connBuf.Bytes()
			frames := 0
			for len(data) > 0 {
				if len(data) < 2 {
					t.Fatalf("truncated frame header: %d bytes left", len(data))
				}
				b1 := data[1]
				masked := b1&0x80 != 0
				payloadLen := int64(b1 & 0x7f)
				hdr := 2
				switch payloadLen {
				case 126:
					payloadLen = int64(data[2])<<8 | int64(data[3])
					hdr = 4
				case 127:
					pl := uint64(0)
					for i := 0; i < 8; i++ {
						pl = pl<<8 | uint64(data[2+i])
					}
					payloadLen = int64(pl)
					hdr = 10
				}
				if masked {
					hdr += 4
				}
				total := int64(hdr) + payloadLen
				if int64(len(data)) < total {
					t.Fatalf("truncated frame: need %d, have %d", total, len(data))
				}
				data = data[total:]
				frames++
			}
			if frames < tt.wantFrames {
				t.Errorf("wrote %d frames, want at least %d", frames, tt.wantFrames)
			}

			// Round-trip through the receiver to confirm reassembly.
			rc := newTestConn(&connBuf, nil, !tt.isServer)
			// Rewind connBuf via new buffer since Bytes() is consumed
			// above; rebuild.
			{
				var buf bytes.Buffer
				wc := newTestConn(nil, &buf, tt.isServer)
				wc.SetWriteFrameSize(tt.frameCap)
				if err := wc.WriteMessage(BinaryMessage, payload); err != nil {
					t.Fatal(err)
				}
				rc = newTestConn(&buf, nil, !tt.isServer)
			}
			_, got, err := rc.ReadMessage()
			if err != nil {
				t.Fatalf("ReadMessage: %v", err)
			}
			if !bytes.Equal(got, payload) {
				t.Errorf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
			}
		})
	}
}

// TestCloseGracefully exercises the initiator side of the RFC 6455 close
// handshake. Two Conns are wired through a net.Pipe; the client calls
// CloseGracefully and the server runs a plain read loop so the default
// close handler echoes the close back.
func TestCloseGracefully(t *testing.T) {
	cConn, sConn := net.Pipe()
	client := newConn(cConn, false, 1024, 1024, nil, nil, nil)
	server := newConn(sConn, true, 1024, 1024, nil, nil, nil)

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		// The default close handler writes a Close reply when the peer's
		// Close frame arrives, then NextReader returns a *CloseError.
		for {
			if _, _, err := server.NextReader(); err != nil {
				break
			}
		}
		_ = server.Close()
	}()

	err := client.CloseGracefully(CloseNormalClosure, "bye", time.Now().Add(2*time.Second))
	if err != nil {
		t.Fatalf("CloseGracefully: %v", err)
	}

	select {
	case <-serverDone:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not observe the close handshake in time")
	}
}

// TestCloseGracefullyTimeout verifies that a peer that ignores the Close
// frame causes CloseGracefully to tear down the connection at the deadline
// rather than hanging indefinitely.
func TestCloseGracefullyTimeout(t *testing.T) {
	cConn, sConn := net.Pipe()
	client := newConn(cConn, false, 1024, 1024, nil, nil, nil)
	// Server side: drain the Close frame into the void but never reply.
	go func() {
		buf := make([]byte, 1024)
		for {
			if _, err := sConn.Read(buf); err != nil {
				return
			}
		}
	}()

	start := time.Now()
	err := client.CloseGracefully(CloseNormalClosure, "bye",
		time.Now().Add(200*time.Millisecond))
	elapsed := time.Since(start)

	if err == nil {
		// Some platforms may return nil if the pipe Close happens to
		// succeed after the deadline expires; not a failure.
		t.Logf("no err returned; elapsed=%v", elapsed)
	}
	if elapsed > 1*time.Second {
		t.Errorf("CloseGracefully took too long: %v", elapsed)
	}
	_ = sConn.Close()
}
