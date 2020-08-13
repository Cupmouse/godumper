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
	conn *websocket.Conn
	// Do not close from public
	Event chan WebSocketEventData
	// Do not close from public
	Send     chan []byte
	readErr  chan error
	writeErr chan error
}

// To stop, close conn, then close stop
func websocketReadRoutine(conn *websocket.Conn, logger *log.Logger, event chan WebSocketEventData, stop chan struct{}, errc chan error) {
	var err error
	defer func() {
		if err != nil {
			errc <- err
		}
		close(errc)
	}()
	defer func() {
		if rec := recover(); rec != nil {
			if err != nil {
				err = fmt.Errorf("recover: %v, originally: %v", rec, err)
			} else {
				err = fmt.Errorf("recover: %v", rec)
			}
			logger.Printf("%s", debug.Stack())
		}
	}()
	for {
		typ, msg, serr := conn.ReadMessage()
		now := time.Now()
		if serr != nil {
			err = serr
			return
		}
		if typ == websocket.BinaryMessage {
			err = errors.New("BinaryMessage as messageType is not supported")
			return
		}
		select {
		case event <- WebSocketEventData{
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
func websocketWriteRoutine(conn *websocket.Conn, logger *log.Logger, event chan WebSocketEventData, messages chan []byte, stop chan struct{}, errc chan error) {
	var err error
	defer func() {
		if err != nil {
			errc <- err
		}
		close(errc)
	}()
	defer func() {
		if rec := recover(); rec != nil {
			if err != nil {
				err = fmt.Errorf("recover: %v, originally: %v", rec, err)
			} else {
				err = fmt.Errorf("recover: %v", rec)
			}
			logger.Printf("%s", debug.Stack())
		}
	}()
	for {
		select {
		case msg, ok := <-messages:
			if !ok {
				// The channel closed
				return
			}
			serr := conn.WriteMessage(websocket.TextMessage, msg)
			now := time.Now()
			if serr != nil {
				err = serr
				return
			}
			select {
			case event <- WebSocketEventData{
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

// websocketRoutine controls write/read routine.
// `errc` will be closed if this routine stopped executing.
func websocketRoutine(conn *websocket.Conn, logger *log.Logger, event chan WebSocketEventData, send chan []byte, stop chan struct{}, errc chan error) {
	var err error
	defer func() {
		if err != nil {
			errc <- err
		}
		close(errc)
		close(event)
	}()
	connClosed := false
	defer func() {
		if !connClosed {
			serr := conn.Close()
			connClosed = true
			if serr != nil {
				if err != nil {
					err = fmt.Errorf("conn close: %v, originally: %v", serr, err)
				} else {
					err = fmt.Errorf("conn close: %v", serr)
				}
			}
		}
	}()
	rwstop := make(chan struct{})
	readErr := make(chan error)
	go websocketReadRoutine(conn, logger, event, rwstop, readErr)
	defer func() {
		// Same as below but for a read routine
		serr, ok := <-readErr
		if ok {
			if err != nil {
				err = fmt.Errorf("read: %v, originally: %v", serr, err)
			} else {
				err = fmt.Errorf("read: %v", serr)
			}
		}
	}()
	writeErr := make(chan error)
	go websocketWriteRoutine(conn, logger, event, send, rwstop, writeErr)
	defer func() {
		// Wait for a write routine to stop by either closing the channel or returning error
		serr, ok := <-writeErr
		if ok {
			if err != nil {
				err = fmt.Errorf("write: %v, originally: %v", serr, err)
			} else {
				err = fmt.Errorf("write: %v", serr)
			}
		}
	}()
	defer close(rwstop)
	defer func() {
		// This will stop read/write
		connClosed = true
		serr := conn.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("conn close: %v, originally: %v", serr, err)
			} else {
				err = fmt.Errorf("conn close: %v", serr)
			}
		}
	}()
	defer func() {
		if rec := recover(); rec != nil {
			if err != nil {
				err = fmt.Errorf("recover: %v, originally: %v", rec, err)
			} else {
				err = fmt.Errorf("recover: %v", rec)
			}
			logger.Printf("%s", debug.Stack())
		}
	}()
	select {
	case serr := <-readErr:
		err = fmt.Errorf("read: %v", serr)
	case serr := <-writeErr:
		err = fmt.Errorf("writer: %v", serr)
	case <-stop:
		break
	}
}

// NewWebSocket connects to a WebSocket server.
// An error occurred at the creation of a connection is returned immidiately.
// `chan WebSocketEventData` will be closed after `chan error` is closed.
// `chan []byte` is the channel to send messages.
// `chan WebSocketEventData` as well as `chan error` must be watched when sending a message.
// Must only be closed after `chan error` is closed.
// Be careful when sending a message, one also have to look for an error in case blocked.
// `chan struct{}` is closed by the caller when the connection should be closed.
// Note that it won't be closed on its own, only be closed by the caller.
// `chan error` will be used to report a future error on message write/read and be closed after the connection closed
// to indicate all resources associated with the connection are freed.
func NewWebSocket(us string, logger *log.Logger, eventBufferSize int, sendBufferSize int) (chan WebSocketEventData, chan []byte, chan struct{}, chan error, error) {
	// Connect to the WebSocket server
	dialer := websocket.Dialer{
		WriteBufferSize:  1024 * 32,
		ReadBufferSize:   1024 * 32,
		HandshakeTimeout: 5 * time.Second,
	}
	conn, res, serr := dialer.Dial(us, nil)
	if serr != nil {
		return nil, nil, nil, nil, serr
	}
	if res.StatusCode != 101 {
		err := fmt.Errorf("connect to WebSocket server failed: status code %d", res.StatusCode)
		serr := conn.Close()
		if serr != nil {
			err = fmt.Errorf("%v, original error was: %v", serr, err)
		}
		return nil, nil, nil, nil, err
	}
	event := make(chan WebSocketEventData)
	send := make(chan []byte, sendBufferSize)
	stop := make(chan struct{})
	errc := make(chan error)
	go websocketRoutine(conn, logger, event, send, stop, errc)
	return event, send, stop, errc, nil
}
