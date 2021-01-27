package queue

import "net"

// Command holds the parsed command and a pointer to the connection to write the response
type Command struct {
	Conn    *net.Conn
	Command string
	Args    []string
}

// Queue is a FIFO event loop
var Queue chan Command

func init() {
	// will allow 100 commands to queue up before blocking or replying with busy
	Queue = make(chan Command, 100)
}

// Enqueue adds a command to the event loop
func Enqueue(conn net.Conn, command string, args []string) {
	// TODO: add a select and return a busy error
	// so that clients can back off and retry
	Queue <- Command{
		Conn:    &conn,
		Command: command,
		Args:    args,
	}
}
