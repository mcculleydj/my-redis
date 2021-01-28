package handler

import (
	"bufio"
	"fmt"
	"io"
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
	// TODO: handle arrays, if required
}

// HandleConnection is responsible for reading bytes
// passing data to specific store level functions
// constructing a reply for the client
// and closing the connection on completion
func HandleConnection(conn net.Conn) {
	fmt.Println("Handling new connection...")
	reader := bufio.NewReader(conn)

	for {
		s, err := reader.ReadString('\n')
		if err != nil {
			conn.Write([]byte("-malformed RESP: missing delimiter*\r\n"))
			continue
		}
		token := strings.Trim(s, "\r\n")

		if token[0] != '*' {
			conn.Write([]byte("-unexpected input: RESP string does not begin with *\r\n"))
			continue
		}

		nargs, err := parseLength(token)
		if err != nil {
			conn.Write([]byte("-unexpected input: request length is not a parsable integer\r\n"))
			continue
		}

		var command string
		var args []string
		errFlag := false

		for i := 0; i < nargs; i++ {
			s, err := reader.ReadString('\n')
			if err != nil {
				conn.Write([]byte("-malformed RESP: missing delimiter\r\n"))
				errFlag = true
				break
			}
			token := strings.Trim(s, "\r\n")

			if token[0] == '$' {
				size, err := parseLength(token)
				if err != nil {
					conn.Write([]byte("-malformed RESP: unable to parse length\r\n"))
					errFlag = true
					break
				}
				s, err = parseBulkString(reader, size)
				if err != nil {
					conn.Write([]byte("-malformed RESP: unable to parse bulk string\r\n"))
					errFlag = true
					break
				}
			} else {
				if err != nil {
					conn.Write([]byte("-malformed RESP: command array should only contain bulk strings\r\n"))
					errFlag = true
					break
				}
			}

			if i == 0 {
				command = strings.ToLower(s)
			} else {
				args = append(args, strings.ToLower(s))
			}
		}

		if errFlag {
			continue
		}

		queue.Enqueue(conn, command, args)
		fmt.Printf("Finished parsing a new %s command...\n", strings.ToUpper(command))
	}
}

// HandleCommand performs the command specified by the request
func HandleCommand(conn net.Conn, command string, args []string) error {
	return commandMap[command](conn, args)
}
