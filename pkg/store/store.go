package store

import (
	"fmt"
	"net"
	"strconv"
	"time"
)

var store map[string]string
var expiry map[string]time.Time

func init() {
	store = map[string]string{}
	expiry = map[string]time.Time{}
}

// Get gets a key value from the store
func Get(conn net.Conn, args []string) (err error) {
	if v, ok := store[args[0]]; !ok {
		// "$-1\r\n" is RESP's null
		_, err = conn.Write([]byte("$-1\r\n"))
	} else {
		// check expiration
		// TODO: this lazy (on-access) delete is not sufficient
		//       to protect against filling available memory with
		//       expired keys that should be GC'd
		if exp, ok := expiry[args[0]]; ok && time.Now().After(exp) {
			_, err = conn.Write([]byte("$-1\r\n"))
			delete(store, args[0])
			return
		}
		_, err = conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)))
	}
	return
}

// Set puts a key value in the store
func Set(conn net.Conn, args []string) (err error) {
	// TODO: implement set ex
	// based on prompt will only support EX flag
	// not handling user syntax errors in this clone
	// will assume "set foo bar" or "set foo bar ex 1"
	store[args[0]] = args[1]
	if len(args) > 2 {
		var duration int
		duration, err = strconv.Atoi(args[3])
		if err != nil {
			conn.Write([]byte("-expiry must be an integer\r\n"))
			return nil
		}
		expiry[args[0]] = time.Now().Add(time.Duration(duration) * time.Second)
	}
	_, err = conn.Write([]byte("+OK\r\n"))
	return
}
