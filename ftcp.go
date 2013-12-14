/*
Package ftcp implements a basic framed messaging protocol over TCP/TLS, based on
github.com/oxtoacart/framed.

ftcp can work with both plain text connections (see Dial()) and TLS connections
(see DialTLS()).

Connections opened with Dial() or DialTLS() automatically redial whenever
they encounter an error.  If the redial can't successfully complete within
DEFAULT_REDIAL_TIMEOUT milliseconds, then the connection gives up and returns an
error.

Example:

	package main

	import (
		"github.com/oxtoacart/ftcp"
		"log"
	)

	func main() {
		// Replace host:port with an actual TCP server, for example the echo service
		if conn, err := ftcp.Dial("host:port"); err == nil {
			if err := framedConn.Write([]byte("Hello World")); err == nil {
				if msg, err := framedConn.Read(); err == nil {
					log.Println("We're done!")
				}
			}
		}
	}
*/
package ftcp

import (
	"crypto/tls"
	"fmt"
	"github.com/oxtoacart/framed"
	"io"
	"net"
	"time"
)

const (
	DEFAULT_WRITE_QUEUE_DEPTH = 1000
	BACKOFF_MS                = 20
	DEFAULT_REDIAL_TIMEOUT    = 60000
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
	addr          string              // the host:port which the Conn will dial
	redialTimeout time.Duration       // timeout for attempting redials
	tlsConfig     *tls.Config         // (optional) TLS configuration for dialing
	autoRedial    bool                // whether or not to auto-redial
	orig          interface{}         // the original connection (net.Conn or tls.Conn)
	stream        *framed.Framed      // framed version of orig
	writeCh       chan []byte         // channel to which frames to be written are sent
	messages      chan Message        // channel from which received frames are read
	readErrors    chan error          // channel for reporting errors that happened while reading
	writeErrors   chan error          // channel for reporting errors that happened while writing
	readError     chan error          // channel for signaling to the process method that it needs to handle an error from the read method
	nextStream    chan *framed.Framed // channel for telling the read goroutine about the next stream from which it should read (after redialing)
	stop          chan interface{}    // channel for signaling to the process method that it should stop
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
func Dial(addr string) (conn *Conn, err error) {
	conn = newConn(addr)
	conn.autoRedial = true
	if err = conn.dial(); err == nil {
		conn.run()
	}
	return
}

/*
Dial opens a TLS connection to the given address with the given (optional)
tls.Config, similarly to tls.Dial.
*/
func DialTLS(addr string, config *tls.Config) (conn *Conn, err error) {
	conn = newConn(addr)
	conn.autoRedial = true
	conn.tlsConfig = config
	if conn.tlsConfig == nil {
		conn.tlsConfig = new(tls.Config)
	}
	if err = conn.dial(); err == nil {
		conn.run()
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
Accept accepts a new connection, similarly to net.Listener.Accept.
*/
func (listener *Listener) Accept() (conn *Conn, err error) {
	var orig net.Conn
	if orig, err = listener.Listener.Accept(); err == nil {
		conn = newConn("")
		conn.orig = &orig
		conn.stream = &framed.Framed{orig}
		conn.run()
	}
	return
}

/*
Write requests a write of the given message frame to the connection.

If the connection is autoRedial, this write will be queued for delivery after
redial can be successfully completed.  If the number of queued messages equals
the WRITE_QUEUE_DEPTH, Write will block until the queue can start to be drained
again.

If the connection is not autoRedial, Write returns any error encountered while
trying to write to the connection.
*/
func (conn *Conn) Write(msg []byte) (err error) {
	select {
	case err = <-conn.writeErrors:
		return
	default:
		conn.writeCh <- msg
		return
	}
}

/*
Read reads the next message to arrive on the connection.

If the connection is autoRedial, Read will never return an error and instead
simply block until we're able to read something.

If the connection is not autoRedial, Read will return any error encountered
while trying to read from the connection.
*/
func (conn *Conn) Read() (msg Message, err error) {
	select {
	case msg = <-conn.messages:
		return
	case err = <-conn.readErrors:
		return
	}
}

/*
Close closes the connection.
*/
func (conn *Conn) Close() (err error) {
	// Stop the goroutines
	conn.stopReading()
	conn.stopProcessing()
	
	// Close the underlying connection
	switch orig := conn.orig.(type) {
	case *net.Conn:
		err = (*orig).Close()
	case *tls.Conn:
		err = orig.Close()
	}
	
	return
}

/*
SetDeadline sets the read and write deadlines on the underlying connection.
*/
func (conn *Conn) SetDeadline(t time.Time) error {
	switch orig := conn.orig.(type) {
	case net.TCPConn:
		return orig.SetDeadline(t)
	case tls.Conn:
		return orig.SetDeadline(t)
	default:
		return fmt.Errorf("Unable to SetDeadline on connection {}", orig)
	}
}

/*
SetReadDeadline sets the read deadline on the underlying connection.
*/
func (conn *Conn) SetReadDeadline(t time.Time) error {
	switch orig := conn.orig.(type) {
	case net.TCPConn:
		return orig.SetReadDeadline(t)
	case tls.Conn:
		return orig.SetReadDeadline(t)
	default:
		return fmt.Errorf("Unable to SetReadDeadline on connection {}", orig)
	}
}

/*
SetWriteDeadline sets the write deadline on the underlying connection.
*/
func (conn *Conn) SetWriteDeadline(t time.Time) error {
	switch orig := conn.orig.(type) {
	case net.TCPConn:
		return orig.SetWriteDeadline(t)
	case tls.Conn:
		return orig.SetWriteDeadline(t)
	default:
		return fmt.Errorf("Unable to SetWriteDeadline on connection {}", orig)
	}
}

/*
newConn creates a new Conn with sensible defaults.
*/
func newConn(addr string) (conn *Conn) {
	return &Conn{
		addr:          addr,
		redialTimeout: DEFAULT_REDIAL_TIMEOUT,
		writeCh:       make(chan []byte, DEFAULT_WRITE_QUEUE_DEPTH),
		messages:      make(chan Message),
		readErrors:    make(chan error),
		writeErrors:   make(chan error),
		readError:     make(chan error),
		nextStream:    make(chan *framed.Framed),
		stop:          make(chan interface{}),
	}
}

/*
dial dials the connection and returns any error encountered while doing so.
*/
func (conn *Conn) dial() (err error) {
	var orig net.Conn
	if conn.tlsConfig != nil {
		orig, err = tls.Dial("tcp", conn.addr, conn.tlsConfig)
	} else {
		orig, err = net.Dial("tcp", conn.addr)
	}

	if err == nil {
		conn.orig = &orig
		conn.stream = &framed.Framed{orig}
	}

	return
}

/*
redial redials the connection.

As long as dial fails, redial increases the supplied backoff by a factor of 2
and tries again.  If the time elapsed exceeds RETRY_TIMEOUT before dial
succeeds, redial returns the most recent error from dial.
*/
func (conn *Conn) redial(start time.Time, backoff time.Duration) (err error) {
	for {
		if time.Now().Sub(start) > conn.redialTimeout {
			// We're done trying to back off, just return the error
			return
		}
		if backoff > 0 {
			// Not our first try, wait a little
			time.Sleep(backoff * time.Millisecond)
		}
		if err = conn.dial(); err == nil {
			// Redial successful
			return
		} else {
			// Dial failed, bump up the backoff
			if backoff == 0 {
				backoff = BACKOFF_MS
			} else {
				backoff *= 2
			}
		}
	}
}

/*
run starts the goroutines for reading and processing the connection.
*/
func (conn *Conn) run() {
	go conn.read(conn.stream)
	go conn.process()
}

/*
read reads from the given stream.  It is intended to run in a goroutine.  Doing
our reads on a single goroutine ensures that length prefixes and their
corresponding frames are read in the correct order, allowing Read to be called
from multiple goroutines.

Reading is interrupted by sending a message to conn.nextStream.  If the message
is nil, reading simply stops.  If the message is a stream, then reading stops
and a new goroutine is launched to read from the new stream.  This is used on
autoRedial connections in which a write error triggered a redial.
*/
func (conn *Conn) read(stream *framed.Framed) {
	for {
		select {
		case nextStream := <-conn.nextStream:
			if nextStream != nil {
				go conn.read(nextStream)
			}
			return
		default:
			if frame, err := conn.stream.ReadFrame(); err != nil {
				// Unable to read, report error for handling in process()
				conn.readError <- err
				return
			} else {
				// Read succeeded, report message
				var connectionState tls.ConnectionState
				switch orig := conn.orig.(type) {
				case *tls.Conn:
					connectionState = orig.ConnectionState()
				}
				conn.messages <- Message{frame, connectionState}
			}
		}
	}
}

/*
restartReading restarts the reading go routine by sending the conn's new stream
to the nextStream channel.
*/
func (conn *Conn) restartReading() {
	conn.nextStream <- conn.stream
}

/*
stopReading stops the reading go routine by sending a nil stream to the
nextStream channel.
*/
func (conn *Conn) stopReading() {
	conn.nextStream <- nil
}

/*
process handles writes and error handling on a single goroutine.
*/
func (conn *Conn) process() {
	for {
		select {
		case <-conn.stop:
			return
		case readError := <-conn.readError:
			if conn.autoRedial {
				if readError == io.EOF {
					if redialErr := conn.redial(time.Now(), 0); redialErr == nil {
						go conn.read(conn.stream)
					} else {
						// Unable to redial, just report the error and stop processing
						conn.readErrors <- readError
						return
					}
				}
			} else {
				// No autoRedial, just report the error and stop processing
				conn.readErrors <- readError
				return
			}
		case frame := <-conn.writeCh:
			redialed, err := conn.writeFrame(frame)
			if err != nil {
				// Writing failed, stop processing
				conn.stopReading()
				conn.writeErrors <- err
				return
			} else if redialed {
				// Had to redial, restart reading on the new stream
				conn.restartReading()
			}
		}
	}
}

/*
stopProcessing stops processing by sending a message to the stop channel.
*/
func (conn *Conn) stopProcessing() {
	conn.stop <- nil
}

/*
writeFrame writes the frame to the connection.

On an error, if the connection is not autoRedial, writeFrame reports the error
and stops.

If the connection is autoRedial and the writeFrame encounters EOF, writeFrame
tries to redial().

If redialing isn't attempted or doesn't work, an error is returned.
*/
func (conn *Conn) writeFrame(frame []byte) (redialed bool, err error) {
	for {
		var start = time.Now()
		var backoff time.Duration
		if err = conn.stream.WriteFrame(frame); err == nil {
			return
		} else {
			if conn.autoRedial && err == io.EOF {
				err = conn.redial(start, backoff)
				redialed = true
				return
			}
		}
	}
}
