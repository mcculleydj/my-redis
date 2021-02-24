package handler

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"github.com/mcculleydj/my-redis/pkg/queue"
	"github.com/mcculleydj/my-redis/pkg/store"
	"github.com/mcculleydj/my-redis/pkg/util"
)

var commandMap map[string]func(net.Conn, []string) error

func init() {
	commandMap = map[string]func(net.Conn, []string) error{
		"command": util.Command,
		"ping":    util.Ping,
		"echo":    util.Echo,
		"get":     store.Get,
		"set":     store.Set,
	}
}

// RESP prefix definitions
// + simple string
// - error
// : integer
// $ bulk string
// * array

// strip the first character and parse the number of bytes or array length
func parseLength(s string) (size int, err error) {
	return strconv.Atoi(strings.TrimPrefix(s, string(s[0])))
}

// + and : for simple string and integer
// are valid RESP, but client -> server requests
// from the Redis CLI are all of the form:
// *1\r\n$7\r\nCOMMAND\r\n$N\r\nARG\r\n...
// making these two functions superfluous

// func parseSimpleString(token string) string {
// 	return strings.TrimPrefix(token, "+")
// }

// func parseInteger(token string) (int, error) {
// 	return strconv.Atoi(strings.TrimPrefix(token, ":"))
// }

// parsing arrays is also not necessary to implement GET and SETEX

func parseBulkString(r *bufio.Reader, size int) (string, error) {
	// +2 accounts for the terminating "\r\n" in RESP
	buf := make([]byte, size+2)
	_, err := io.ReadFull(r, buf)
	return strings.Trim(string(buf), "\r\n"), err
}

// parses a component of the RESP request
// returns the parsed string (either a command or an arg)
// and the byte slice containing the RESP error encountered
func parseComponent(r *bufio.Reader) (string, []byte) {
	s, err := r.ReadString('\n')
	if err != nil {
		return "", []byte("-malformed RESP: missing delimiter\r\n")
	}

	token := strings.Trim(s, "\r\n")

	if token[0] == '$' {
		size, err := parseLength(token)
		if err != nil {
			return "", []byte("-malformed RESP: unable to parse length\r\n")
		}

		s, err = parseBulkString(r, size)
		if err != nil {
			return "", []byte("-malformed RESP: unable to parse bulk string\r\n")
		}
	} else {
		return "", []byte("-malformed RESP: command array should only contain bulk strings\r\n")
	}

	return s, nil
}

// HandleConnection is responsible for:
// - parsing RESP
// - responding with any errors
// - placing the correct op on the event queue
func HandleConnection(conn net.Conn) {
	fmt.Println("Handling new connection...")
	reader := bufio.NewReader(conn)

	for {
		s, err := reader.ReadString('\n')
		if err != nil {
			conn.Write([]byte("-malformed RESP: missing delimiter\r\n"))
			continue
		}

		token := strings.Trim(s, "\r\n")
		if token[0] != '*' {
			conn.Write([]byte("-malformed RESP: request must begin with *\r\n"))
			continue
		}

		nargs, err := parseLength(token)
		if err != nil {
			conn.Write([]byte("-malformed RESP: request length is not a parsable integer\r\n"))
			continue
		}

		var command string
		var args []string
		errFlag := false

		for i := 0; i < nargs; i++ {
			s, bs := parseComponent(reader)
			if bs != nil {
				conn.Write(bs)
				errFlag = true
				break
			} else if i == 0 {
				command = strings.ToLower(s)
			} else {
				args = append(args, strings.ToLower(s))
			}
		}

		if errFlag {
			continue
		}

		// this implementation enqueues based on the order in which commands are parsed
		// not the order in which those commands arrive at the server
		// if it was necessary to execute commands in order of their arrival
		// then requests must be queued and single threading occurs earlier
		// given that networks are inherently async, endeavoring to align
		// order of execution with order of arrival seems like a waste
		queue.Enqueue(conn, command, args)
		fmt.Printf("Finished parsing a new %s command...\n", strings.ToUpper(command))
	}
}

// HandleCommand performs the command specified by op in the queue
func HandleCommand(conn net.Conn, command string, args []string) error {
	// GetExp is internal and there is no associated client connection
	if command == "getexp" {
		fmt.Println("calling getexp for", args[0])
		store.GetExp(args[0])
		return nil
	}
	return commandMap[command](conn, args)
}
