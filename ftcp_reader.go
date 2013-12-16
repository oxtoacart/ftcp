package ftcp

/*
Reader encapsulates one of potentially multiple readers that are reading from
the Connection.
*/
type Reader struct {
	conn       *Conn        // the connection to which this Reader is tied
	in         chan Message // channel from which received messages are read
	readErrors chan error   // channel for reporting errors that happened while reading
	added      chan bool    // channel to notify Reader that it has been added to conn
	removed    chan bool    // channel to notify Reader that it has been removed from conn
}

/*
Read reads the next message to arrive on the connection.

If the connection is autoRedial, Read will never return an error and instead
simply block until we're able to read something.

If the connection is not autoRedial, Read will return any error encountered
while trying to read from the connection.
*/
func (reader *Reader) Read() (msg Message, err error) {
	select {
	case msg = <-reader.in:
		return
	case err = <-reader.readErrors:
		return
	}
}

/*
Close closes this reader.
*/
func (reader *Reader) Close() {
	reader.conn.removeReader <- reader
}
