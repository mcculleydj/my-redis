package store

import (
	"fmt"
	"net"
)

var store map[string]string

func init() {
	store = map[string]string{}
}

// Get gets a key value from the store
func Get(conn net.Conn, args []string) (err error) {
	if v, ok := store[args[0]]; !ok {
		// "$-1\r\n" is RESP's null
		_, err = conn.Write([]byte("$-1\r\n"))
	} else {
		_, err = conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)))
	}
	return
}

// Set puts a key value in the store
func Set(conn net.Conn, args []string) error {
	store[args[0]] = args[1]
	_, err := conn.Write([]byte("+OK\r\n"))
	return err
}
