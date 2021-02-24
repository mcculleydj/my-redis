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
// and will remove any expired values from the store and expiry map
func Get(conn net.Conn, args []string) (err error) {
	if v, ok := store[args[0]]; !ok {
		// "$-1\r\n" is RESP's null
		_, err = conn.Write([]byte("$-1\r\n"))
	} else {
		if GetExp(args[0]) {
			_, err = conn.Write([]byte("$-1\r\n"))
			return
		}
		_, err = conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)))
	}
	return
}

// GetExp allows for key expiration checking without
// a client connection since this is an internal process
// this will be called independently of its appearance in Get
// since the CheckExp loop will place getexp ops on the event queue
func GetExp(key string) (expired bool) {
	// check expiry map
	mutex.RLock()
	exp, ok := expiry[key]
	mutex.RUnlock()

	if expired = ok && time.Now().After(exp); expired {
		// ops with store RW access are only invoked
		// as part of the single threaded event queue
		// no need to lock
		delete(store, key)

		// background CheckExp process has read access
		// to the expiry map requiring a write lock
		mutex.Lock()
		delete(expiry, key)
		mutex.Unlock()
	}

	return expired
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
		// lock required because CheckExp process will need to iterate over
		// expiry map to sample for expired keys in a separate thread
		mutex.Lock()
		expiry[args[0]] = time.Now().Add(time.Duration(duration) * time.Second)
		mutex.Unlock()
	}

	_, err = conn.Write([]byte("+OK\r\n"))

	return
}

// CheckExp will sample the expiry map in a sep Go routine
// and will place GetExp ops onto the queue to cull expired keys
// without any risk of a race condition with client ops
// the key may or may not be expired by the time GetExp is called
// as a result of an incoming client SETEX op
func CheckExp() {
	// configuration values which may have significant performance impacts
	// percentage above which delay will half provided a full sample could be taken
	const threshold float32 = .1
	// delay floor to avoid mutex from blocking client ops too often
	const minDelay = 1 * time.Second
	// delay ceiling to avoid a piling up of expired keys
	const maxDelay = 512 * time.Second
	// number of SETEX keys to check
	// TODO: a more sophisticated implementation might increase sample size
	//       as the percentage of expired keys in each sample grows
	//       as it may be the case that checking 1000 keys every second is not
	//       enough to keep up the growth of expired keys
	const sampleSize = 1000
	// starting delay
	delay := 1 * time.Second

	for {
		// delay will range between 2^0 and 2^9 seconds
		time.Sleep(delay)
		count := 0
		expiredCount := 0

		// RW mutex allows any GET op that doesn't need to
		// mutate the expiry map to proceed
		mutex.RLock()
		for k, v := range expiry {
			count++
			if time.Now().After(v) {
				expiredCount++
				queue.Enqueue(nil, "getexp", []string{k})
			}
			if count >= sampleSize {
				break
			}
		}
		mutex.RUnlock()

		// exponential backoff logic:
		// calculate the percentage expired
		// if threshold exceeded and sample is complete (count == sampleSize)
		// then decrease the delay between checks by half (no lower than 1 second)
		// in all other cases double the delay (no more than 512 seconds)
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

// TODO: write some tests to check exp backoff functionality under real loads
