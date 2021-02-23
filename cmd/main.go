package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"

	"github.com/mcculleydj/my-redis/pkg/handler"
	"github.com/mcculleydj/my-redis/pkg/queue"
)

func main() {
	// listen on TCP port 6379
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

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

	// this implementation enqueues based on the order in which commands are parsed
	// not the order in which those commands arrive at the server
	// if it was necessary to execute commands in order of their arrival
	// then we would need to queue requests and start single threading at the parse step
	go func() {
		for c := range queue.Queue {
			err := handler.HandleCommand(*c.Conn, c.Command, c.Args)
			if err != nil {
				log.Println("HandleCommand err:", err.Error())
			}
		}
	}()

	fmt.Println("Listening on port 6379...")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	close(queue.Queue)
	fmt.Println("\nSIGTERM received...")
}

// can ctrl-c to stop the execution of a sigterm, so this:
/*
	func main() {
    util.Initialize()
    go api.Start(*flags.Port)
    manager.Start()
    c := make(chan os.Signal, 1)
    done := make(chan bool, 1)
    signal.Notify(c, os.Interrupt)
    go func() {
        for range c {
            if err := cleanup(); err != nil {
                color.Println("red", "shutdown failure: "+err.Error())
                continue
            }
            close(c)
            break
        }
        done <- true
    }()
    <-done
}
*/
