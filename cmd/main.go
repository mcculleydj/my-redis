package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"

	"github.com/mcculleydj/my-redis/pkg/handler"
	"github.com/mcculleydj/my-redis/pkg/queue"
	"github.com/mcculleydj/my-redis/pkg/store"
)

func main() {
	// listen on Redis' default port
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	// server loop -- responds to each request in a separate Go routine
	// but all ops end up on a queue and are handled by a single thread
	go func() {
		for {
			// wait for a connection
			conn, err := l.Accept()
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()
			go handler.HandleConnection(conn)
		}
	}()

	// execution loop -- synchronously pull ops off channel and pass them to handler
	go func() {
		for c := range queue.Queue {
			err := handler.HandleCommand(*c.Conn, c.Command, c.Args)
			if err != nil {
				log.Println("HandleCommand err:", err.Error())
			}
		}
	}()

	go store.CheckExp()

	fmt.Println("Listening on port 6379...")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	close(queue.Queue)
	fmt.Println("\nSIGTERM received...")
}
