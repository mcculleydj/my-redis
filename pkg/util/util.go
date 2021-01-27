package util

import (
	"fmt"
	"net"
)

// Command responds to the initial redis-cli request
// with RESP's representation for an empty array
func Command(conn net.Conn, args []string) error {
	_, err := conn.Write([]byte("*0\r\n"))
	return err
}

// Ping Pongs
func Ping(conn net.Conn, args []string) error {
	_, err := conn.Write([]byte("+PONG\r\n"))
	return err
}

// Echo sends back the provided message
func Echo(conn net.Conn, args []string) error {
	// TODO: handle too many args
	_, err := conn.Write([]byte(fmt.Sprintf("+%s\r\n", args[0])))
	return err
}
