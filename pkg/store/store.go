package store

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/mcculleydj/my-redis/pkg/queue"
)

var store map[string]string
var expiry map[string]time.Time
var mutex sync.RWMutex

func init() {
	store = map[string]string{}
	expiry = map[string]time.Time{}
	mutex = sync.RWMutex{}
}

// Get retrieves a key value from the store
func Get(conn net.Conn, args []string) (err error) {
	if v, ok := store[args[0]]; !ok {
		// "$-1\r\n" is RESP's null
		_, err = conn.Write([]byte("$-1\r\n"))
	} else {

		// check expiration
		mutex.RLock()
		exp, ok := expiry[args[0]]
		mutex.RUnlock()

		if ok && time.Now().After(exp) {
			_, err = conn.Write([]byte("$-1\r\n"))
			delete(store, args[0])
			mutex.Lock()
			delete(expiry, args[0])
			mutex.Unlock()
			return
		}
		_, err = conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)))
	}
	return
}

// Set puts a key value in the store with an optional expiry
func Set(conn net.Conn, args []string) (err error) {
	// based on prompt will only support EX flag
	// not handling user syntax errors in this clone
	// will assume "set foo bar" or "set foo bar ex 1"

	// upsert the key value in the store
	store[args[0]] = args[1]

	// if expiry exists
	if len(args) > 2 {
		var duration int
		duration, err = strconv.Atoi(args[3])
		if err != nil {
			conn.Write([]byte("-expiry must be an integer\r\n"))
			return nil
		}
		// save expiry on the expiry map to be checked by background process
		// lock because gc process will need to iterate over
		// map kvs to sample for expired keys
		mutex.Lock()
		expiry[args[0]] = time.Now().Add(time.Duration(duration) * time.Second)
		mutex.Unlock()
	}

	_, err = conn.Write([]byte("+OK\r\n"))

	return
}

// Check will sample the expiry map (will be run as a sep Go routine)
// and place Get ops onto the queue to cull expired keys
// without any risk of a race condition with client ops

// should have like a mock connection? or allow get calls w/o conn?
func Check(conn net.Conn) {
	// tunable metrics which could
	// have significant performance impacts
	const threshold float32 = .1
	const minDelay = 1 * time.Second
	const maxDelay = 512 * time.Second
	const sampleSize = 1000
	delay := 1 * time.Second

	for {
		time.Sleep(delay)
		count := 0
		expiredCount := 0

		// will be able to handle all ops except for set ex
		mutex.RLock()
		for k, v := range expiry {
			count++
			if time.Now().After(v) {
				expiredCount++
				queue.Enqueue(conn, "get", []string{k})
			}
			if count >= sampleSize {
				break
			}
		}
		mutex.RUnlock()

		// exponential backoff implementation
		// calculate the percentage expired
		// if it exceeds the threshold and we have enough keys in expiry map
		// to yield a full sample decrease the delay between checks
		// no lower than the minimum delay
		// in all other cases double the delay
		// no greater than the maximum delay
		percentageExpired := float32(expiredCount) / float32(count)
		if percentageExpired > threshold && count >= sampleSize {
			if delay != minDelay {
				delay /= 2
			}
		} else if delay != maxDelay {
			delay *= 2
		}
	}
}
