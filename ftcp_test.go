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
	var receivedIn2 Message
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
				reader := conn.Reader()
				//defer reader.Close()
				if msg, err := reader.Read(); err == nil {
					receivedOut = msg
					msgOut := Message{RepID: msg.ID, Data: []byte(expectedIn)}
					if err := conn.Write(msgOut); err != nil {
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
		if err != nil {
			errFromGoroutine = err
			return
		}
		defer conn.Close()
		if err != nil {
			errFromGoroutine = fmt.Errorf("Unable to dial address: %s %s", addr, err)
			return
		}
		msgOut := Message{ID: expectedId, Data: []byte(expectedOut)}

		// Read using a reader on one Goroutine
		go func() {
			reader := conn.Reader()
			//defer reader.Close()
			if msgIn2, err := reader.Read(); err != nil {
				errFromGoroutine = fmt.Errorf("Error reading response with Reader: %s", err)
			} else {
				receivedIn2 = msgIn2
			}
		}()

		time.Sleep(100 * time.Millisecond)
		// Send a request/reply message
		receivedIn, err = conn.Req(msgOut, 500 * time.Millisecond)

		if forceClose {
			// Wait and write again in case the original message got buffered but not delivered
			time.Sleep(200 * time.Millisecond)
			conn.Write(msgOut)
		}
	}()

	time.Sleep(250 * time.Millisecond)
	if errFromGoroutine != nil {
		t.Fatal(errFromGoroutine)
	}
	if string(receivedOut.Data) != expectedOut {
		t.Fatalf("Sent payload did not match expected.  Expected '%s', Received '%s'", expectedOut, string(receivedOut.Data))
	}
	if receivedOut.ID != expectedId {
		t.Fatalf("Sent ID did not match expected.  Expected '%s', Received '%s'", expectedId, receivedOut.ID)
	}
	for i, recvd := range []Message{receivedIn, receivedIn2} {
		if string(recvd.Data) != expectedIn {
			t.Fatalf("Response payload %d did not match expected.  Expected '%s', Received '%s'", i, expectedIn, string(recvd.Data))
		}
		if recvd.RepID != expectedId {
			t.Fatalf("Response RepID %d did not match expected.  Expected '%s', Received '%s'", i, expectedId, recvd.RepID)
		}
	}

}
