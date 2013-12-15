package ftcp

import (
	"fmt"
	"testing"
	"time"
)

func TestWithoutClose(t *testing.T) {
	doTest(t, false)
}

func TestWithClose(t *testing.T) {
	doTest(t, true)
}

func doTest(t *testing.T, forceClose bool) {
	var expectedOut = "Hello framed world"
	var expectedId = MessageID(59)
	var expectedIn = "Hello caller!"
	var receivedOut Message
	var receivedIn Message
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
				errFromGoroutine = fmt.Errorf("Unable to accept: %s", err)
			} else {
				if first && forceClose {
					conn.Close()
					first = false
					continue
				}
				if msg, err := conn.Read(); err == nil {
					receivedOut = msg
					if err := conn.Write(Message{RepID: msg.ID, Data: []byte(expectedIn)}); err != nil {
						errFromGoroutine = err
					}
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
		msgOut := Message{ID: expectedId, Data: []byte(expectedOut)}
		if err := conn.Write(msgOut); err != nil {
			errFromGoroutine = err
			return
		}
		if forceClose {
			// Wait and write again in case the original message got buffered but not delivered
			time.Sleep(500 * time.Millisecond)
			conn.Write(msgOut)
		}
		if msgIn, err := conn.Read(); err != nil {
			errFromGoroutine = fmt.Errorf("Error reading response: %s", err)
		} else {
			receivedIn = msgIn
		}
	}()

	time.Sleep(1000 * time.Millisecond)
	if errFromGoroutine != nil {
		t.Fatal(errFromGoroutine)
	}
	if string(receivedOut.Data) != expectedOut {
		t.Fatalf("Sent payload did not match expected.  Expected '%s', Received '%s'", expectedOut, string(receivedOut.Data))
	}
	if receivedOut.ID != expectedId {
		t.Fatalf("Sent ID did not match expected.  Expected '%s', Received '%s'", expectedId, receivedOut.ID)
	}
	if string(receivedIn.Data) != expectedIn {
		t.Fatalf("Response payload did not match expected.  Expected '%s', Received '%s'", expectedIn, string(receivedIn.Data))
	}
	if receivedIn.RepID != expectedId {
		t.Fatalf("Response RepID did not match expected.  Expected '%s', Received '%s'", expectedId, receivedIn.RepID)
	}
}
