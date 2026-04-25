// Copyright 2013 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Command server is a test server for the Autobahn WebSockets Test Suite.
package main

import (
	"flag"
	"io"
	"log"
	"net/http"

	"github.com/scalecode-solutions/magilla"
)

var upgrader = magilla.Upgrader{
	ReadBufferSize:    4096,
	WriteBufferSize:   4096,
	EnableCompression: true,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// echoCopy echoes messages from the client using io.Copy. magilla's
// built-in UTF-8 validation on TextMessage streams takes care of the
// RFC 6455 §6.1 fail-fast requirement — the reader returns an error
// (and the library sends Close 1007) the moment an invalid sequence
// appears, so there's no application-level validator here.
func echoCopy(w http.ResponseWriter, r *http.Request, writerOnly bool) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade:", err)
		return
	}
	defer conn.Close()
	for {
		mt, r, err := conn.NextReader()
		if err != nil {
			if err != io.EOF {
				log.Println("NextReader:", err)
			}
			return
		}
		w, err := conn.NextWriter(mt)
		if err != nil {
			log.Println("NextWriter:", err)
			return
		}
		if writerOnly {
			_, err = io.Copy(struct{ io.Writer }{w}, r)
		} else {
			_, err = io.Copy(w, r)
		}
		if err != nil {
			log.Println("Copy:", err)
			return
		}
		err = w.Close()
		if err != nil {
			log.Println("Close:", err)
			return
		}
	}
}

func echoCopyWriterOnly(w http.ResponseWriter, r *http.Request) {
	echoCopy(w, r, true)
}

func echoCopyFull(w http.ResponseWriter, r *http.Request) {
	echoCopy(w, r, false)
}

// echoReadAll echoes messages from the client by reading the entire
// message with io.ReadAll. magilla's built-in UTF-8 validation on
// TextMessage payloads means the ReadMessage call itself returns an
// error (and the library emits Close 1007) for invalid sequences —
// no application-level utf8.Valid check is needed.
func echoReadAll(w http.ResponseWriter, r *http.Request, writeMessage, writePrepared bool) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade:", err)
		return
	}
	defer conn.Close()
	for {
		mt, b, err := conn.ReadMessage()
		if err != nil {
			if err != io.EOF {
				log.Println("NextReader:", err)
			}
			return
		}
		if writeMessage {
			if !writePrepared {
				err = conn.WriteMessage(mt, b)
				if err != nil {
					log.Println("WriteMessage:", err)
				}
			} else {
				pm, err := magilla.NewPreparedMessage(mt, b)
				if err != nil {
					log.Println("NewPreparedMessage:", err)
					return
				}
				err = conn.WritePreparedMessage(pm)
				if err != nil {
					log.Println("WritePreparedMessage:", err)
				}
			}
		} else {
			w, err := conn.NextWriter(mt)
			if err != nil {
				log.Println("NextWriter:", err)
				return
			}
			if _, err := w.Write(b); err != nil {
				log.Println("Writer:", err)
				return
			}
			if err := w.Close(); err != nil {
				log.Println("Close:", err)
				return
			}
		}
	}
}

func echoReadAllWriter(w http.ResponseWriter, r *http.Request) {
	echoReadAll(w, r, false, false)
}

func echoReadAllWriteMessage(w http.ResponseWriter, r *http.Request) {
	echoReadAll(w, r, true, false)
}

func echoReadAllWritePreparedMessage(w http.ResponseWriter, r *http.Request) {
	echoReadAll(w, r, true, true)
}

func serveHome(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.Error(w, "Not found.", http.StatusNotFound)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	io.WriteString(w, "<html><body>Echo Server</body></html>")
}

var addr = flag.String("addr", ":9000", "http service address")

func main() {
	flag.Parse()
	http.HandleFunc("/", serveHome)
	http.HandleFunc("/c", echoCopyWriterOnly)
	http.HandleFunc("/f", echoCopyFull)
	http.HandleFunc("/r", echoReadAllWriter)
	http.HandleFunc("/m", echoReadAllWriteMessage)
	http.HandleFunc("/p", echoReadAllWritePreparedMessage)
	err := http.ListenAndServe(*addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

