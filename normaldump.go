package main

import (
	"fmt"
	"log"
)

// NormalDump is the interface of normaldump compatibles
type NormalDump interface {
	BeforeConnect() error
	Subscribe() ([][]byte, error)
}

func dumpNormal(exchange string, us string, directory string, alwaysDisk bool, logger *log.Logger,
	d NormalDump, stop chan struct{}) (err error) {
	if d != nil {
		serr := d.BeforeConnect()
		if serr != nil {
			err = fmt.Errorf("before connect: %v", serr)
			return
		}
	}
	writer, werr := NewWriter(exchange, us, directory, alwaysDisk, logger, 10000)
	defer func() {
		close(writer)
		serr, ok := <-werr
		if ok {
			if err != nil {
				err = fmt.Errorf("writer: %v, originally: %v", serr, err)
			} else {
				err = fmt.Errorf("writer: %v", serr)
			}
		}
	}()
	event, send, wsstop, wserr, serr := NewWebSocket(us, logger, 10000, 10000)
	if serr != nil {
		err = serr
		return
	}
	defer func() {
		close(wsstop)
		serr, ok := <-wserr
		if ok {
			if err != nil {
				err = fmt.Errorf("writer: %v, originally: %v", serr, err)
			} else {
				err = fmt.Errorf("writer: %v", serr)
			}
		}
	}()
	logger.Println("sending subscribe")
	subs, serr := d.Subscribe()
	if serr != nil {
		err = fmt.Errorf("subscribe: %v", serr)
		return
	}
	for _, sub := range subs {
		select {
		case send <- sub:
		case event := <-event:
			var m WriterQueueMethod
			switch event.Type {
			case WebSocketEventReceive:
				m = WriteMessage
			case WebSocketEventSend:
				m = WriteSend
			}
			select {
			case writer <- WriterQueueElement{
				method:    m,
				timestamp: event.Timestamp,
				message:   event.Message,
			}:
			case serr := <-werr:
				err = fmt.Errorf("writer: %v", serr)
				return
			case <-stop:
				return
			}
		case serr := <-wserr:
			err = fmt.Errorf("websocket: %v", serr)
			return
		}
	}
	logger.Println(exchange, "dumping")
	for {
		select {
		case e := <-event:
			var m WriterQueueMethod
			switch e.Type {
			case WebSocketEventReceive:
				m = WriteMessage
			case WebSocketEventSend:
				m = WriteSend
			}
			select {
			case writer <- WriterQueueElement{
				method:    m,
				timestamp: e.Timestamp,
				message:   e.Message,
			}:
			case serr := <-werr:
				err = fmt.Errorf("writer: %v", serr)
				return
			case <-stop:
				return
			}
		case serr := <-werr:
			err = fmt.Errorf("writer: %v", serr)
			return
		case serr := <-wserr:
			err = fmt.Errorf("websocket: %v", serr)
			return
		case <-stop:
			// Stop signal received
			return
		}
	}
}
