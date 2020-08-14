package main

import (
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"time"

	"github.com/gorilla/websocket"
)

// WebsocketEvent is type of event in WebSocket connection.
type WebsocketEvent int

const (
	// WebSocketEventReceive is the event that a message was received.
	WebSocketEventReceive = WebsocketEvent(0)
	// WebSocketEventSend is the event that a message was send.
	WebSocketEventSend = WebsocketEvent(1)
)

// WebSocketEventData is event happened in WebSocket connection.
type WebSocketEventData struct {
	Type      WebsocketEvent
	Timestamp time.Time
	Message   []byte
}

// WebSocketClient is WebSocket client wrapper.
// Must be closed.
type WebSocketClient struct {
	conn      *websocket.Conn
	logger    *log.Logger
	events    chan *WebSocketEventData
	send      chan []byte
	stop      chan struct{}
	errc      chan error
	lastError error
	closed    bool
}

// To stop, close conn, then close stop
func (c *WebSocketClient) readRoutine(stop chan struct{}, errc chan error) {
	var err error
	defer func() {
		if err != nil {
			errc <- fmt.Errorf("read: %v", err)
		}
		close(errc)
	}()
	defer func() {
		if rec := recover(); rec != nil {
			if err != nil {
				err = fmt.Errorf("read: recover: %v, originally: %v", rec, err)
			} else {
				err = fmt.Errorf("read: recover: %v", rec)
			}
			c.logger.Printf("%s", debug.Stack())
		}
	}()
	for {
		typ, msg, serr := c.conn.ReadMessage()
		now := time.Now()
		if serr != nil {
			err = serr
			return
		}
		if typ == websocket.BinaryMessage {
			err = errors.New("read: BinaryMessage as messageType is not supported")
			return
		}
		select {
		case c.events <- &WebSocketEventData{
			Type:      WebSocketEventReceive,
			Timestamp: now,
			Message:   msg,
		}:
		case <-stop:
			// Force stop
			return
		}
	}
}

// To stop, close conn, then close stop
func (c *WebSocketClient) writeRoutine(stop chan struct{}, errc chan error) {
	var err error
	defer func() {
		if err != nil {
			errc <- fmt.Errorf("write: %v", err)
		}
		close(errc)
	}()
	defer func() {
		if rec := recover(); rec != nil {
			if err != nil {
				err = fmt.Errorf("write: recover: %v, originally: %v", rec, err)
			} else {
				err = fmt.Errorf("write: recover: %v", rec)
			}
			c.logger.Printf("%s", debug.Stack())
		}
	}()
	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				// The channel closed
				return
			}
			err = c.conn.WriteMessage(websocket.TextMessage, msg)
			now := time.Now()
			if err != nil {
				return
			}
			select {
			case c.events <- &WebSocketEventData{
				Type:      WebSocketEventSend,
				Timestamp: now,
				Message:   msg,
			}:
			case <-stop:
				return
			}
		case <-stop:
			return
		}
	}
}

// websocketRoutine manages write/read routine.
// `errc` will be closed if this routine stopped executing.
func (c *WebSocketClient) websocketRoutine() {
	var err error
	defer func() {
		// This have to be closed before c.errc so others know error was happened
		close(c.events)
		if err != nil {
			c.errc <- fmt.Errorf("websocket: %v", err)
		}
		close(c.errc)
	}()
	rwstop := make(chan struct{})
	readErr := make(chan error)
	go c.readRoutine(rwstop, readErr)
	defer func() {
		// Same as below but for a read routine
		serr, ok := <-readErr
		if ok {
			if err != nil {
				err = fmt.Errorf("%v, originally: %v", serr, err)
			} else {
				err = serr
			}
		}
	}()
	writeErr := make(chan error)
	go c.writeRoutine(rwstop, writeErr)
	defer func() {
		// Wait for a write routine to stop by either closing the channel or returning error
		serr, ok := <-writeErr
		if ok {
			if err != nil {
				err = fmt.Errorf("%v, originally: %v", serr, err)
			} else {
				err = serr
			}
		}
	}()
	defer func() {
		// This will stop read/write
		serr := c.conn.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("conn close: %v, originally: %v", serr, err)
			} else {
				err = fmt.Errorf("conn close: %v", serr)
			}
		}
		close(rwstop)
	}()
	select {
	case err = <-readErr:
	case err = <-writeErr:
	case <-c.stop:
	}
}

// Send adds send jobs to the queue.
// returns false as `success` if the queue is full or an error is returned.
// Always returns an error if the client is closed.
func (c *WebSocketClient) Send(msg []byte) (success bool, err error) {
	if c.closed {
		return false, errors.New("websocket: already closed")
	}
	if c.lastError != nil {
		return false, c.lastError
	}
	select {
	case c.send <- msg:
		return true, nil
	case serr, ok := <-c.errc:
		if ok {
			c.lastError = serr
			return false, serr
		}
		return false, errors.New("websocket: connection unavailable")
	default:
		// Send queue is full
		return false, nil
	}
}

// Event returns channel to receive event.
// If returned channel is closed, then check for an error using `Error`.
func (c *WebSocketClient) Event() chan *WebSocketEventData {
	return c.events
}

// Error returns the last reported error from
func (c *WebSocketClient) Error() error {
	if c.closed {
		return errors.New("websocket: already closed")
	}
	serr, ok := <-c.errc
	if ok {
		c.lastError = serr
		return serr
	}
	return c.lastError
}

// Close closes the client and frees resources associated with.
// If the client has already been closed, it returns the last error.
func (c *WebSocketClient) Close() error {
	if c.closed {
		return c.lastError
	}
	close(c.stop)
	c.closed = true
	serr, ok := <-c.errc
	if ok {
		c.lastError = serr
		return serr
	}
	return nil
}

// NewWebSocket connects to a WebSocket server.
// An error occurred at the creation of a connection is returned immidiately.
func NewWebSocket(us string, logger *log.Logger, eventBufferSize int, sendBufferSize int) (*WebSocketClient, error) {
	// Connect to the WebSocket server
	dialer := websocket.Dialer{
		WriteBufferSize:  1024 * 32,
		ReadBufferSize:   1024 * 32,
		HandshakeTimeout: 5 * time.Second,
	}
	conn, res, serr := dialer.Dial(us, nil)
	if serr != nil {
		return nil, serr
	}
	if res.StatusCode != 101 {
		err := fmt.Errorf("websocket: status code %d", res.StatusCode)
		serr := conn.Close()
		if serr != nil {
			err = fmt.Errorf("%v, original error was: %v", serr, err)
		}
		return nil, err
	}
	c := new(WebSocketClient)
	c.conn = conn
	c.errc = make(chan error)
	c.events = make(chan *WebSocketEventData, eventBufferSize)
	c.send = make(chan []byte, sendBufferSize)
	c.stop = make(chan struct{})
	c.logger = logger
	go c.websocketRoutine()
	return c, nil
}
