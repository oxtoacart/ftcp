package ftcp

import (
	"testing"
	"time"
)

func TestPlain(t *testing.T) {
	var expectedOut = "Hello framed world"
	var expectedIn = "Hello caller!"

	listener, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Unable to listen: %s", err)
	}
	addr := listener.Addr().String()

	// Accept connections, read message and respond
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				t.Fatalf("Unable to accept: %s", err)
			}
			if msg, err := conn.Read(); err != nil {
				t.Fatalf("Unable to read request: %s", err)
			} else {
				out := string(msg.data)
				if out != expectedOut {
					t.Fatalf("Sent payload did not match expected.  Expected '%s', Received '%s'", expectedOut, out)
				}
				conn.Write([]byte(expectedIn))
			}
		}
	}()

	// Write message
	conn, err := Dial(addr)
	if err != nil {
		t.Fatalf("Unable to dial address: %s", addr)
	}
	time.Sleep(1000)
	conn.Write([]byte(expectedOut))
	if msg, err := conn.Read(); err != nil {
		t.Fatalf("Error reading response: %s", err)
	} else {
		in := string(msg.data)
		if in != expectedIn {
			t.Fatalf("Response payload did not match expected.  Expected '%s', Received '%s'", expectedIn, in)
		}
	}
}
