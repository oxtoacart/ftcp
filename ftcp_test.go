package ftcp

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestPlain(t *testing.T) {
	var expectedOut = "Hello framed world"
	var expectedIn = "Hello caller!"
	var receivedOut string
	var receivedIn string
	var errFromGoroutine error

	listener, err := Listen("127.0.0.1:0")
	defer listener.Close()
	if err != nil {
		t.Fatalf("Unable to listen: %s", err)
	}
	addr := listener.Addr().String()

	// Accept connections, read message and respond
	go func() {
		first := true
		for {
			conn, err := listener.Accept()
			if err != nil {
				errFromGoroutine = err
				errFromGoroutine = fmt.Errorf("Unable to accept: %s", err)
			} else {
				if first {
					conn.Close()
					first = false
					continue
				}
				if msg, err := conn.Read(); err == nil {
					receivedOut = string(msg.data)
					conn.Write([]byte(expectedIn))
				}
				return
			}
		}
	}()

	// Write message
	go func() {
		conn, err := Dial(addr)
		defer conn.Close()
		if err != nil {
			errFromGoroutine = fmt.Errorf("Unable to dial address: %s %s", addr, err)
			return
		}
		conn.Write([]byte(expectedOut))
		// Wait and write again in case the original message got buffered but not delivered
		time.Sleep(500 * time.Millisecond)
		conn.Write([]byte(expectedOut))
		if msg, err := conn.Read(); err != nil {
			errFromGoroutine = fmt.Errorf("Error reading response: %s", err)
		} else {
			receivedIn = string(msg.data)
		}
	}()

	time.Sleep(1000 * time.Millisecond)
	if errFromGoroutine != nil {
		t.Fatal(errFromGoroutine)
	}
	if receivedOut != expectedOut {
		t.Fatalf("Sent payload did not match expected.  Expected '%s', Received '%s'", expectedOut, receivedOut)
	}

	if receivedIn != expectedIn {
		t.Fatalf("Response payload did not match expected.  Expected '%s', Received '%s'", expectedIn, receivedIn)
	}

	os.Exit(0)
}
