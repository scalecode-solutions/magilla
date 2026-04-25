// Copyright 2015 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build ignore

package main

import (
	"context"
	"flag"
	"log"
	"net/url"
	"os"
	"os/signal"
	"time"

	"github.com/scalecode-solutions/magilla"
)

var addr = flag.String("addr", "localhost:8080", "http service address")

func main() {
	flag.Parse()
	log.SetFlags(0)

	// signal.NotifyContext binds Ctrl-C to ctx cancellation, so any
	// blocking ReadMessageContext / WriteMessageContext call unblocks
	// cleanly when the user interrupts.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	u := url.URL{Scheme: "ws", Host: *addr, Path: "/echo"}
	log.Printf("connecting to %s", u.String())

	c, _, err := magilla.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessageContext(ctx)
			if err != nil {
				log.Println("read:", err)
				return
			}
			log.Printf("recv: %s", message)
		}
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case t := <-ticker.C:
			if err := c.WriteMessageContext(ctx, magilla.TextMessage, []byte(t.String())); err != nil {
				log.Println("write:", err)
				return
			}
		case <-ctx.Done():
			log.Println("interrupt")
			// CloseGracefully encapsulates the full RFC 6455 close
			// handshake: send Close, drain reads until peer echoes
			// (or deadline), tear down the net.Conn.
			if err := c.CloseGracefully(magilla.CloseNormalClosure, "", time.Now().Add(time.Second)); err != nil {
				log.Println("close:", err)
			}
			return
		}
	}
}
