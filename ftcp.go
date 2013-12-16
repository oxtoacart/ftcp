/*
Package ftcp implements a basic framed messaging protocol over TCP/TLS, based on
github.com/oxtoacart/framed.

ftcp can work with both plain text connections (see Dial()) and TLS connections
(see DialTLS()).

Connections opened with Dial() or DialTLS() automatically redial whenever
they encounter an error.  If the redial can't successfully complete within
DEFAULT_REDIAL_TIMEOUT milliseconds, then the connection gives up and returns an
error.

One writes to connections directly using Conn.Write().

One reads from connections by obtaining a Reader from Conn.Reader() and then
using Reader.Read().  Make sure to close Readers using the Close() function
after you're done reading, otherwise other Readers will block!

One can also use synchronous request/reply semantics with Conn.Req().

Example:

	package main

	import (
		"github.com/oxtoacart/ftcp"
		"log"
		"time"
	)

	func main() {
		// Replace host:port with an actual TCP server, for example the echo service
		if conn, err := ftcp.Dial("host:port"); err == nil {
			// Construct a Message to send
			msg := Message{Data: []byte("Hello World")}
			
			// Write directly on the Conn
			if err := framedConn.Write(msg); err == nil {
				// Read using a Reader
				reader := framedConn.Reader()
				defer reader.Close()
				if msg, err := reader.Read(); err == nil {
					log.Println("Received message: {}", msg)
				}
			}
			
			// Alternately, use request/reply semantics
			if repMsg, err := framedConn.Req(msg, 500 * time.Millisecond); err == nil {
				log.Println("Received reply: {}", repMsg)
			}
		}
	}
*/
package ftcp

import (
	"crypto/tls"
	"encoding/binary"
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

var (
	ErrTimeout = fmt.Errorf("Operation timed out")
)

type MessageID uint64

/*
Message encapsulates a message received from an ftcp connection, including an
id, the data (payload) of the message and, if received via TLS, the
tls.ConnectionState.

If left unspecified, the ID is auto-assigned based on a sequential number.
*/
type Message struct {
	ID       MessageID // The ID of this message
	RepID    MessageID // The ID of the message to which this message is replying
	Data     []byte
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
	readers       []*Reader           // readers reading from this Connection (there is always at least 1, the default reader)
	frameRead     chan []byte         // channel for frame that was read
	addReader     chan *Reader        // channel for requests to add readers
	removeReader  chan *Reader        // channel for requests to remove readers
	out           chan Message        // channel to which messages to be written are sent
	writeErrors   chan error          // channel for reporting errors that happened while writing
	readError     chan error          // channel for signaling to the process method that it needs to handle an error from the read method
	nextStream    chan *framed.Framed // channel for telling the read goroutine about the next stream from which it should read (after redialing)
	stop          chan interface{}    // channel for signaling to the process method that it should stop
	idSeq         uint64              // sequence number for assigning IDs to messages
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
Write requests a write of the given message to the connection.

If the connection is autoRedial, this write will be queued for delivery after
redial can be successfully completed.  If the number of queued messages equals
the WRITE_QUEUE_DEPTH, Write will block until the queue can start to be drained
again.

If the connection is not autoRedial, Write returns any error encountered while
trying to write to the connection.
*/
func (conn *Conn) Write(msg Message) (err error) {
	select {
	case err = <-conn.writeErrors:
		return
	default:
		conn.out <- msg
		return
	}
}

/*
Creates a new Reader on the given conn.  A Reader allows multiple goroutines to
read from the same Conn in parallel.

IMPORTANT - Once you have opened a Reader, you need to Read() from it to drain
incoming messages on the Conn, otherwise it will block other Readers.  When
finished reading, close the reader with Close().
*/
func (conn *Conn) Reader() (reader *Reader) {
	reader = &Reader{
		conn:       conn,
		in:         make(chan Message),
		readErrors: make(chan error),
		added:      make(chan bool),
		removed:    make(chan bool),
	}
	// Request Reader to be added to Conn
	conn.addReader <- reader
	// Wait for Reader to be added to Conn
	<-reader.added
	return
}

/*
Req implements blocking request/reply semantics on top of a Conn.
*/
func (conn *Conn) Req(req Message, timeout time.Duration) (rep Message, err error) {
	repCh := make(chan Message)
	errCh := make(chan error)
	reader := conn.Reader()
	defer reader.Close()

	// Read the reply
	go func() {
		for {
			if rep, err := reader.Read(); err != nil {
				errCh <- err
				return
			} else {
				if rep.RepID == req.ID {
					repCh <- rep
				}
				return
			}
		}
	}()

	// Send the request
	conn.Write(req)

	select {
	case rep = <-repCh:
		return rep, nil
	case err = <-errCh:
		return
	case <-time.After(timeout):
		err = ErrTimeout
		return
	}

	return
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
	conn = &Conn{
		addr:          addr,
		redialTimeout: DEFAULT_REDIAL_TIMEOUT,
		frameRead:     make(chan []byte),
		addReader:     make(chan *Reader, 100),
		removeReader:  make(chan *Reader, 100),
		out:           make(chan Message, DEFAULT_WRITE_QUEUE_DEPTH),
		writeErrors:   make(chan error),
		readError:     make(chan error),
		nextStream:    make(chan *framed.Framed),
		stop:          make(chan interface{}),
	}
	return conn
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
				// Read succeeded, report frame
				conn.frameRead <- frame
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
process is our traffic cop - anything requiring synchronization goes through
here.
*/
func (conn *Conn) process() {
	for {
		select {
		case <-conn.stop:
			return
		case reader := <-conn.addReader:
			conn.handleAddReader(reader)
		case reader := <-conn.removeReader:
			conn.handleRemoveReader(reader)
		case msg := <-conn.out:
			conn.handleRecvdMsg(msg)
		case frame := <-conn.frameRead:
			conn.handleFrameRead(frame)
		case readError := <-conn.readError:
			conn.handleReadError(readError)
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
func (conn *Conn) writeFrame(byteArrays ...[]byte) (redialed bool, err error) {
	for {
		var start = time.Now()
		var backoff time.Duration
		if err = conn.stream.WriteFrame(byteArrays...); err == nil {
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

func (conn *Conn) handleReadError(readError error) {
	if conn.autoRedial {
		if readError == io.EOF {
			if redialErr := conn.redial(time.Now(), 0); redialErr == nil {
				go conn.read(conn.stream)
			} else {
				// Unable to redial, just report the error and stop processing
				for _, reader := range conn.readers {
					reader.readErrors <- readError
				}
				return
			}
		}
	} else {
		// No autoRedial, just report the error and stop processing
		for _, reader := range conn.readers {
			reader.readErrors <- readError
		}
		return
	}
}

func (conn *Conn) handleRecvdMsg(msg Message) {
	idBytes := make([]byte, 8)
	repIdBytes := make([]byte, 8)
	if msg.ID == 0 {
		conn.idSeq += 1
		msg.ID = MessageID(conn.idSeq)
	}
	if n := binary.PutUvarint(idBytes, uint64(msg.ID)); n <= 0 {
		conn.writeErrors <- fmt.Errorf("Unable to encode ID bytes!")
	} else {
		if n := binary.PutUvarint(repIdBytes, uint64(msg.RepID)); n <= 0 {
			conn.writeErrors <- fmt.Errorf("Unable to encode RepID bytes!")
		} else {
			redialed, err := conn.writeFrame(idBytes, repIdBytes, msg.Data)
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
handleAddReader adds a reader to this Conn.
*/
func (conn *Conn) handleAddReader(reader *Reader) {
	conn.readers = append(conn.readers, reader)
	reader.added <- true
}

/*
handleRemoveReader removes the given Reader from this connection.
*/
func (conn *Conn) handleRemoveReader(reader *Reader) {
	for i, existing := range conn.readers {
		if reader == existing {
			conn.readers = append(conn.readers[:i], conn.readers[i+1:]...)
			return
		}
	}
}

/*
handleFrameRead turns a frame into a Message and reports it.
*/
func (conn *Conn) handleFrameRead(frame []byte) {
	idBytes := frame[0:8]
	repIdBytes := frame[8:16]
	data := frame[16:]
	id, n := binary.Uvarint(idBytes)
	if n <= 0 {
		conn.readError <- fmt.Errorf("Unable to decode ID bytes")
	} else {
		repId, n := binary.Uvarint(repIdBytes)
		if n <= 0 {
			conn.readError <- fmt.Errorf("Unable to decode RepID bytes")
		} else {
			var connectionState tls.ConnectionState
			switch orig := conn.orig.(type) {
			case *tls.Conn:
				connectionState = orig.ConnectionState()
			}
			for _, reader := range conn.readers {
				reader.in <- Message{MessageID(id), MessageID(repId), data, connectionState}
			}
		}
	}
}
