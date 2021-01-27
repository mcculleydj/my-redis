package handler

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"my-redis/pkg/queue"
	"my-redis/pkg/store"
	"my-redis/pkg/util"
	"net"
	"strconv"
	"strings"
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

// strip the first character and parse the integer number of bytes or array length
func parseLength(s string) (size int, err error) {
	return strconv.Atoi(strings.TrimPrefix(s, string(s[0])))
}

// + and : for simple string and integer
// are valid RESP, but client -> server requests
// are all of the form *1\r\n$7\r\nCOMMAND\r\n$N\r\nARG\r\n...

// func parseSimpleString(token string) string {
// 	return strings.TrimPrefix(token, "+")
// }

// func parseInteger(token string) (int, error) {
// 	return strconv.Atoi(strings.TrimPrefix(token, ":"))
// }

func parseBulkString(r *bufio.Reader, size int) (string, error) {
	// +2 accounts for the "\r\n"
	buf := make([]byte, size+2)
	_, err := io.ReadFull(r, buf)
	return strings.Trim(string(buf), "\r\n"), err
}

// unlike parseBulkString, size will be the length of the array
func parseArray(r *bufio.Reader, size int) {
	// TODO: handle arrays
}

// HandleConnection is responsible for reading bytes
// passing data to specific store level functions
// constructing a reply for the client
// and closing the connection on completion
func HandleConnection(conn net.Conn) {
	fmt.Println("Handling new connection...")
	reader := bufio.NewReader(conn)

	// TODO: better error handling; currently ctrl-c from the redis-cli results in log fatal

	for {
		s, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}
		token := strings.Trim(s, "\r\n")

		if token[0] != '*' {
			_, err := conn.Write([]byte("-unexpected input: RESP string does not begin with *\r\n"))
			if err != nil {
				log.Fatal(err)
			}
		}

		nargs, err := parseLength(token)
		if err != nil {
			_, err := conn.Write([]byte("-unexpected input: request length is not a parsable integer\r\n"))
			if err != nil {
				log.Fatal(err)
			}
		}

		var command string
		var args []string

		for i := 0; i < nargs; i++ {
			s, err := reader.ReadString('\n')
			if err != nil {
				log.Fatal(err)
			}
			token := strings.Trim(s, "\r\n")

			switch token[0] {
			case '$':
				size, err := parseLength(token)
				if err != nil {
					log.Fatal(err)
				}
				s, err = parseBulkString(reader, size)
				if err != nil {
					log.Fatal(err)
				}
			case '*':
				// TODO: is this even a valid case for this simple Redis clone
				// supporting set with expiry and get?
				length, err := parseLength(token)
				if err != nil {
					log.Fatal(err)
				}
				parseArray(reader, length)
			default:
				conn.Write([]byte("-unable to parse command\r\n"))
			}

			if i == 0 {
				command = strings.ToLower(s)
			} else {
				args = append(args, strings.ToLower(s))
			}
		}

		queue.Enqueue(conn, command, args)
		fmt.Printf("Finished parsing a new %s command...\n", strings.ToUpper(command))
	}
}

// HandleCommand performs the command specified by the request
func HandleCommand(conn net.Conn, command string, args []string) error {
	return commandMap[command](conn, args)
}
