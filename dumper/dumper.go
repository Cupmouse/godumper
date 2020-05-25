package dumper

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/exchangedataset/godumper/subscriber"
	"github.com/exchangedataset/godumper/writer"
	"github.com/gorilla/websocket"
)

// WriterQueueCapacity is the maximum capacity of queue for writer thread
// if queue exceeded this capacity, then caller thread would be blocked
const WriterQueueCapacity = 10000

type queueMethod int

const (
	// MESSAGE method
	MESSAGE = queueMethod(0)
	// SEND method
	SEND = queueMethod(1)
	// ERROR method
	ERROR = queueMethod(2)
	// EOS method
	EOS = queueMethod(3)
)

type queueElement struct {
	timestamp int64
	method    queueMethod
	message   []byte
}

// to stop writer thread, close queue channel.
// error from errCh ensures the thread can be terminated
func writerThread(conn *websocket.Conn, w *writer.Writer, queue chan queueElement, errCh chan error) {
	var err error
	defer func() {
		if err != nil {
			errCh <- err
		}
		close(errCh)
	}()
	for {
		elem, ok := <-queue
		if !ok {
			// queue channel is closed, stop this thread
			return
		}
		// there is an element in queue
		switch elem.method {
		case MESSAGE:
			err = w.Message(elem.timestamp, elem.message)
			break
		case SEND:
			err = w.Send(elem.timestamp, elem.message)
			break
		case ERROR:
			err = w.Error(elem.timestamp, elem.message)
			break
		case EOS:
			err = w.Close(elem.timestamp)
			break
		default:
			err = errors.New("unknown queue element method")
			return
		}
		if err != nil {
			// send error through channel and leave
			return
		}
	}
}

// returning error via channel ensures the thread can be terminated
func subscribeThread(conn *websocket.Conn, subber subscriber.Subscriber, writer chan queueElement, stop chan bool, errCh chan error) {
	var err error
	defer func() {
		if err != nil {
			errCh <- err
		}
		close(errCh)
	}()
	var subscribes []subscriber.Subscribe
	subscribes, err = subber.Subscribe()
	if err != nil {
		return
	}
	for _, sub := range subscribes {
		select {
		case <-stop:
			// recieved stop signal
			return
		default:
			// continue
			break
		}

		err = conn.WriteMessage(websocket.TextMessage, sub.Message)
		if err != nil {
			// report error and leave
			return
		}
		now := time.Now().UnixNano()
		writer <- queueElement{timestamp: now, method: SEND, message: sub.Message}
	}
}

func readThread(conn *websocket.Conn, subber subscriber.Subscriber, writer chan queueElement, errCh chan error) {
	var err error
	defer func() {
		// this defer function will take care of sending errCh an error from err variable
		// no need to send error through errCh from now on, just put error on err variable
		if err != nil {
			errCh <- err
		}
		close(errCh)
	}()

	for {
		var messageType int
		var message []byte
		messageType, message, err = conn.ReadMessage()
		now := time.Now().UnixNano()
		if err != nil {
			return
		}
		if messageType == websocket.BinaryMessage {
			err = errors.New("BinaryMessage as messageType is not supported")
			return
		}

		// keep in mind this could be block-ed if channel is overflown
		writer <- queueElement{timestamp: now, method: MESSAGE, message: message}
	}
}

// Dump establish connection to the server and dumps WebSocket message
func Dump(exchange string, directory string, logger *log.Logger, errCh chan error, stop chan bool) {
	defer func() {
		close(errCh)
	}()
	var err error
	defer func() {
		if err != nil {
			errCh <- err
		}
	}()
	defer func() {
		if perr := recover(); perr != nil {
			if err != nil {
				err = fmt.Errorf("panic occurred: %v, but error happened before the panic: %v", perr, err)
			} else {
				err = fmt.Errorf("panic occurred: %v", perr)
			}
		}
	}()

	var subber subscriber.Subscriber
	subber, err = subscriber.GetSubscriber(exchange, logger)
	if err != nil {
		logger.Fatal(err)
	}

	// prepare writer
	var w *writer.Writer
	w, err = writer.NewWriter(exchange, subber.URL(), directory, logger)
	if err != nil {
		return
	}
	defer func() {
		now := time.Now().UnixNano()
		serr := w.Close(now)
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("%v, original error was: %v", serr, err)
			} else {
				err = serr
			}
		}
	}()

	// call subscriber.BeforeConnection
	logger.Println("Performing before connection")
	subber.BeforeConnection()

	// connect to the WebSocket server
	dialer := websocket.Dialer{
		WriteBufferSize:  1024 * 32,
		ReadBufferSize:   1024 * 32,
		HandshakeTimeout: 5 * time.Second,
	}
	logger.Println("Establishing connection")
	var conn *websocket.Conn
	var res *http.Response
	conn, res, err = dialer.Dial(subber.URL(), nil)
	if err != nil {
		return
	}
	defer func() {
		serr := conn.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("%v, original error was: %v", serr, err)
			} else {
				err = serr
			}
		}
	}()
	if res.StatusCode != 101 {
		err = fmt.Errorf("connect to WebSocket server failed: status code %d", res.StatusCode)
		return
	}

	// launch thread to write message to writer
	// this could make timestamp more accurate
	queue := make(chan queueElement, WriterQueueCapacity)
	writerErrCh := make(chan error)
	go writerThread(conn, w, queue, writerErrCh)

	// launch another thread to send subscribe message
	subErrorCh := make(chan error)
	// stop channels are buffered only once
	// target thread might already exited if then its channel will never get polled
	// so do not send signal twice because it might get blocked and go deadlocked
	subStop := make(chan bool, 1)
	go subscribeThread(conn, subber, queue, subStop, subErrorCh)

	// launch thread to receive message
	readErrorCh := make(chan error)
	go readThread(conn, subber, queue, readErrorCh)

	logger.Println("Connection established")

	for {
		shouldStop := false
		select {
		case writerErr := <-writerErrCh:
			err = fmt.Errorf("error on writer thread: %v", writerErr)
			break
		case subError, ok := <-subErrorCh:
			if ok {
				err = fmt.Errorf("error on subscribe thread: %v", subError)
			} else {
				// channel is closed
				// set this to nil not to receive in select in the future
				subErrorCh = nil
			}
			break
		case readError := <-readErrorCh:
			err = fmt.Errorf("error on read thread: %v", readError)
			break
		case <-stop:
			// received stop signal
			shouldStop = true
			break
		}
		if shouldStop || err != nil {
			break
		}
	}

	logger.Println("Stopping")

	// closing the connection will make error on read thread, which will stop the thread with error
	cerr := conn.Close()
	if cerr != nil {
		if err != nil {
			err = fmt.Errorf("error on closing connection: %v, previous error was: %v", cerr, err)
		} else {
			err = fmt.Errorf("error on closing connection: %v", cerr)
		}
	}
	// wait for the read thread to stop
	logger.Println("waiting for read thread to stop")
	readErr, ok := <-readErrorCh
	if ok {
		if err != nil {
			// append error
			err = fmt.Errorf("error on read thread: %v, previous error was: %v", readErr, err)
		} else {
			err = fmt.Errorf("error on read thread: %v", readErr)
		}
	}

	// send stop signal to subscribe thread
	close(subStop)
	if subErrorCh != nil {
		logger.Println("waiting for subscribe thread to stop")
		subErr, ok := <-subErrorCh
		if ok {
			if err != nil {
				err = fmt.Errorf("error on subscribe thread: %v, previous error was: %v", subErr, err)
			} else {
				err = fmt.Errorf("error on subscribe thread: %v", err)
			}
		}
	}

	// closing queue channel will stop writer thread
	// do not close this before read and sub thread stop
	close(queue)
	logger.Println("waiting for writer thread to stop")
	// wait for writer thread to stop
	writerErr, ok := <-writerErrCh
	if ok {
		if err != nil {
			// append error
			err = fmt.Errorf("error on writer thread: %v, previous error was: %v", writerErr, err)
		} else {
			err = fmt.Errorf("error on writer thread: %v", err)
		}
	}

	return
}
