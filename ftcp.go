/*
Package ftcp implements a basic framed messaging protocol over TCP/TLS, based on
github.com/oxtoacart/framed.

ftcp can work with both plain text connections (see Dial()) and TLS connections
(see DialTLS()).

Example:

	package main

	import (
		"github.com/oxtoacart/ftcp"
	)

	func main() {
		// Replace host:port with an actual TCP server, for example the echo service
		if conn, err := ftcp.Dial("host:port"); err == nil {
			framedConn.Write([]byte("Hello World"))
			msg := framedConn.Read()
		}
	}

TODO: add auto-reconnect functionality
*/
package ftcp

import (
	"crypto/tls"
	"github.com/oxtoacart/framed"
	"net"
)

/*
Message encapsulates a message received from an ftcp connection, including both
the data (payload) of the message and, if using TLS, the tls.ConnectionState.
*/
type Message struct {
	data     []byte
	TLSState tls.ConnectionState
}

/*
Conn is an ftcp connection to which one can write []byte frames using Write()
and from which one can receive Messages using Read().

Multiple goroutines may invoke methods on a Conn simultaneously.
*/
type Conn struct {
	stream   framed.Framed
	orig     interface{}
	writeCh  chan []byte
	readCh   chan []byte
	messages chan Message
	errors   chan error
	closed   bool
}

/*
Listener is a thin wrapper around net.Listener that allows accepting new
connections using Accept().
*/
type Listener struct {
	net.Listener
}

/*
Dial opens a tcp connection to the given address, similarly to net.Dial.
*/
func Dial(addr string) (conn Conn, err error) {
	var orig net.Conn
	if orig, err = net.Dial("tcp", addr); err == nil {
		conn = newConn(orig)
	}
	return
}

/*
Dial opens a TLS connection to the given address with the given (optional)
tls.Config, similarly to tls.Dial.
*/
func DialTLS(addr string, config *tls.Config) (conn Conn, err error) {
	var orig *tls.Conn
	if orig, err = tls.Dial("tcp", addr, config); err == nil {
		conn = newConn(orig)
	}
	return
}

/*
Listen listens on a TCP socket at the given listen address, similarly to
net.Listen.
*/
func Listen(laddr string) (listener Listener, err error) {
	if orig, err := net.Listen("tcp", laddr); err == nil {
		listener = Listener{orig}
	}
	return
}

/*
ListenTLS listens on a TLS socket at the given listen address with the given
(optional) tls.Config, similarly to tls.Listen.
*/
func ListenTLS(laddr string, config *tls.Config) (listener Listener, err error) {
	if orig, err := tls.Listen("tcp", laddr, config); err == nil {
		listener = Listener{orig}
	}
	return
}

/*
Accept accepts a new connection on, similarly to net.Listener.Accept.
*/
func (listener *Listener) Accept() (conn Conn, err error) {
	var orig net.Conn
	if orig, err = listener.Listener.Accept(); err == nil {
		conn = newConn(orig)
	}
	return
}

/*
Write requests a write of the given message frame to the connection.
*/
func (conn Conn) Write(msg []byte) {
	conn.writeCh <- msg
}

/*
Read reads the next message to arrive on the connection.
*/
func (conn Conn) Read() (msg Message, err error) {
	select {
	case msg = <-conn.messages:
		return
	case err = <-conn.errors:
		return
	}
}

/*
Close closes the connection.
*/
func (conn Conn) Close() (err error) {
	switch orig := conn.orig.(type) {
	case *net.Conn:
		err = (*orig).Close()
	case *tls.Conn:
		err = orig.Close()
	}
	conn.closed = true
	return
}

/*
newConn creates a new connection and starts reading/writing to it.
*/
func newConn(orig net.Conn) (conn Conn) {
	conn = Conn{
		stream:   framed.Framed{orig},
		orig:     &orig,
		writeCh:  make(chan []byte),
		readCh:   make(chan []byte),
		messages: make(chan Message),
		errors:   make(chan error),
		closed:   false,
	}

	go conn.read()
	go conn.process()

	return
}

/*
Read on goroutine.  Doing our reads on a single goroutine ensures that length
prefixes and their corresponding frames are read in the correct order.
*/
func (conn Conn) read() {
	for conn.closed == false {
		if frame, err := conn.stream.ReadFrame(); err != nil {
			// TODO: catch EOF and try reconnecting
			conn.errors <- err
		} else {
			conn.readCh <- frame
		}
	}
}

/*
Process requests to write and manage connection on a single goroutine.
*/
func (conn Conn) process() {
	for conn.closed == false {
		select {
		case frame := <-conn.writeCh:
			if err := conn.stream.WriteFrame(frame); err != nil {
				// TODO: catch EOF and try reconnecting
				conn.errors <- err
			}
		case frame := <-conn.readCh:
			var connectionState tls.ConnectionState
			switch orig := conn.orig.(type) {
			case *tls.Conn:
				connectionState = orig.ConnectionState()
			}
			conn.messages <- Message{frame, connectionState}
		}
	}
}
