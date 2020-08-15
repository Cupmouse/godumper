package main

import (
	"context"
	"errors"
	"fmt"
	"log"
)

// NormalDump is the interface of normaldump compatibles
type NormalDump interface {
	BeforeConnect() error
	Subscribe() ([][]byte, error)
}

func dumpNormal(ctx context.Context, exchange string, us string, directory string, alwaysDisk bool, logger *log.Logger,
	d NormalDump) (err error) {
	if d != nil {
		serr := d.BeforeConnect()
		if serr != nil {
			err = fmt.Errorf("before connect: %v", serr)
			return
		}
	}
	writer, serr := NewWriter(exchange, us, directory, alwaysDisk, logger, 10000)
	if serr != nil {
		return serr
	}
	defer func() {
		// Close writer
		serr := writer.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("%v, originally: %v", serr, err)
			} else {
				err = serr
			}
		}
	}()
	ws, serr := NewWebSocket(us, logger, 10000, 10000)
	if serr != nil {
		err = serr
		return
	}
	defer func() {
		serr := ws.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("%v, originally: %v", serr, err)
			} else {
				err = serr
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
		success, serr := ws.Send(sub)
		if !success {
			if serr != nil {
				return serr
			}
			return errors.New("send queue overflown")
		}
	}
	logger.Println(exchange, "dumping")
	for {
		select {
		case event, ok := <-ws.Event():
			if !ok {
				err = ws.Error()
				return
			}
			var m WriterQueueMethod
			switch event.Type {
			case WebSocketEventReceive:
				m = WriteMessage
			case WebSocketEventSend:
				m = WriteSend
			}
			err = writer.Queue(&WriterQueueElement{
				method:    m,
				timestamp: event.Timestamp,
				message:   event.Message,
			})
			if err != nil {
				return
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
