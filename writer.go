package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"runtime/debug"
	"time"

	"github.com/exchangedataset/streamcommons"
	"github.com/exchangedataset/streamcommons/simulator"
)

// WriterBufferSize is the size of dataset buffer, 5MB
const WriterBufferSize = 5 * 1024 * 1024

// Writer writes messages to gzipped file
type writer struct {
	closed        bool
	lastTimestamp int64
	fileTimestamp int64
	buffer        *bytes.Buffer
	writer        *bufio.Writer
	gwriter       *gzip.Writer
	exchange      string
	url           string
	directory     string
	alwaysDisk    bool
	logger        *log.Logger
	sim           simulator.Simulator
}

func (w *writer) beforeWrite(timestamp int64) (correctedTimestamp int64, err error) {
	if w.closed {
		err = errors.New("tried to write to an already closed writer")
		return
	}
	// Correct time from going backwards
	if timestamp < w.lastTimestamp {
		// Time is running backwards
		// Probably because of system time correction
		w.logger.Println("timestamp is older than the last observed, substituting it to the last observed value")
		timestamp = w.lastTimestamp
	}
	correctedTimestamp = timestamp
	// It creates new file for every minute
	minute := int64(time.Duration(timestamp) / time.Minute)
	lastMinute := int64(time.Duration(w.lastTimestamp) / time.Minute)
	// Set timestamp as last write time, this have to be after lastMinute is calculated
	w.lastTimestamp = timestamp
	if minute == lastMinute {
		// Continues to use the same stream & file name
		return
	}
	// Time to split dataset
	isFirstFile := w.buffer == nil
	if isFirstFile {
		// Create new buffer
		bufArr := make([]byte, 0, WriterBufferSize)
		w.buffer = bytes.NewBuffer(bufArr)
		// Prepare buffer writer
		w.writer = bufio.NewWriter(w.buffer)
		// Prepare gzip writer
		w.gwriter, err = gzip.NewWriterLevel(w.writer, gzip.BestCompression)
		if err != nil {
			return
		}
		// Write start line
		startLine := fmt.Sprintf("start\t%d\t%s\n", timestamp, w.url)
		_, err = w.gwriter.Write([]byte(startLine))
		if err != nil {
			return
		}
		err = w.sim.ProcessStart([]byte(w.url))
		if err != nil {
			return
		}
	} else {
		// This will flush and write gzip footer
		err = w.gwriter.Close()
		if err != nil {
			return
		}
		err = w.writer.Flush()
		if err != nil {
			return
		}

		// Upload or store datasets
		err = w.uploadOrStore()
		if err != nil {
			return
		}
		// Emptify buffer
		w.buffer.Reset()
		// Don't have to do anything to writer
		// Prepare gzip writer
		w.gwriter, err = gzip.NewWriterLevel(w.writer, gzip.BestCompression)
		if err != nil {
			return
		}
		if minute%10 == 0 {
			// If last digit of minute is 0 then write state snapshot
			var snapshots []simulator.Snapshot
			snapshots, err = w.sim.TakeStateSnapshot()
			for _, s := range snapshots {
				stateLine := fmt.Sprintf("state\t%d\t%s\t%s\n", timestamp, s.Channel, s.Snapshot)
				_, err = w.gwriter.Write([]byte(stateLine))
				if err != nil {
					return
				}
			}
		}
	}
	// Change file timestamp, this is used to generate file name
	w.fileTimestamp = timestamp
	return
}

// This method assumes contents in buffer are complete.
// This means it does not perform flush or closing gzip writer.
// Before writing the contents of buffer.
func (w *writer) uploadOrStore() (err error) {
	// Name for file would be <exchange>_<timestamp>.gz
	fileName := fmt.Sprintf("%s_%d.gz", w.exchange, w.fileTimestamp)
	if !w.alwaysDisk {
		// Try to upload it to s3
		// Creating new reader from original buffer array because if you read bytes from
		// buffer, read bytes will be lost from buffer
		// We might use them later if s3 upload failed
		err = streamcommons.PutS3Object(fileName, bytes.NewReader(w.buffer.Bytes()))
		if err == nil {
			// successful
			w.logger.Println("uploaded to s3:", fileName)
			return
		}
		// If can not be uploaded to s3, then store it in local storage
		w.logger.Printf("Could not be uploaded to s3: %v\n", err)
	}
	// Make directories to store file
	err = os.MkdirAll(w.directory, 0744)
	if err != nil {
		return
	}
	filePath := path.Join(w.directory, fileName)
	var file *os.File
	file, err = os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0744)
	if err != nil {
		return
	}
	defer func() {
		// Defer function to ensure that opened file be closed
		serr := file.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("%v, original error was: %v", serr, err)
			} else {
				err = serr
			}
		}
	}()
	_, err = file.Write(w.buffer.Bytes())
	w.logger.Printf("making new file: %s\n", fileName)
	return
}

// MessageChannelKnown writes message line to writer, but the channel is already known.
func (w *writer) MessageChannelKnown(channel string, timestamp int64, message []byte) (err error) {
	timestamp, err = w.beforeWrite(timestamp)
	if err != nil {
		return
	}
	err = w.sim.ProcessMessageChannelKnown(channel, message)
	if err != nil {
		return
	}
	// write message despite the error (if happened)
	_, err = w.gwriter.Write([]byte(fmt.Sprintf("msg\t%d\t%s\t", timestamp, channel)))
	if err != nil {
		return
	}
	_, err = w.gwriter.Write(message)
	if err != nil {
		return
	}
	_, err = w.gwriter.Write([]byte("\n"))
	return
}

// Message writes msg line to writer. Channel is automatically determined.
func (w *writer) Message(timestamp int64, message []byte) (err error) {
	timestamp, err = w.beforeWrite(timestamp)
	if err != nil {
		return
	}
	var channel string
	channel, err = w.sim.ProcessMessageWebSocket(message)
	if err != nil {
		return
	}
	if channel == "" || channel == streamcommons.ChannelUnknown {
		// simulator could not determine the channel of message
		w.logger.Println("channel is unknown:", string(message))
	}
	// write message despite the error (if happened)
	_, err = w.gwriter.Write([]byte(fmt.Sprintf("msg\t%d\t%s\t", timestamp, channel)))
	if err != nil {
		return
	}
	_, err = w.gwriter.Write(message)
	if err != nil {
		return
	}
	_, err = w.gwriter.Write([]byte("\n"))
	if err != nil {
		return
	}
	return
}

// Send writes send line to writer. Channel is automatically determined.
func (w *writer) Send(timestamp int64, message []byte) (err error) {
	timestamp, err = w.beforeWrite(timestamp)
	if err != nil {
		return
	}
	var channel string
	channel, err = w.sim.ProcessSend(message)
	if channel == "" || channel == streamcommons.ChannelUnknown {
		// simulator could not determine the channel of message
		w.logger.Println("channel is unknown:", string(message))
	}
	_, err = w.gwriter.Write([]byte(fmt.Sprintf("send\t%d\t%s\t", timestamp, channel)))
	if err != nil {
		return
	}
	_, err = w.gwriter.Write(message)
	if err != nil {
		return
	}
	_, err = w.gwriter.Write([]byte("\n"))
	return
}

// Error writes err line to writer.
func (w *writer) Error(timestamp int64, message []byte) (err error) {
	timestamp, err = w.beforeWrite(timestamp)
	if err != nil {
		return
	}
	_, err = w.gwriter.Write([]byte(fmt.Sprintf("err\t%d\t%s\t\n", timestamp, message)))
	return
}

// Close closes this writer and underlying file and gzip writer. It also writes end line.
func (w *writer) Close(timestamp int64) (err error) {
	// already closed
	if w.closed {
		return
	}
	timestamp, err = w.beforeWrite(timestamp)
	if err != nil {
		return
	}
	// report error as it is
	_, err = w.gwriter.Write([]byte(fmt.Sprintf("end\t%d\n", timestamp)))
	// this will also flush buffer in gzip writer
	serr := w.gwriter.Close()
	if serr != nil {
		if err != nil {
			err = fmt.Errorf("error on closing gzip: %v, previous error was: %v", serr, err)
		} else {
			err = fmt.Errorf("error on closing gzip: %v", serr)
		}
	}
	serr = w.writer.Flush()
	if serr != nil {
		if err != nil {
			err = fmt.Errorf("error on flushing writer: %v, previous error was: %v", serr, err)
		} else {
			err = fmt.Errorf("error on flushing writer: %v", serr)
		}
	}
	// don't forget to upload!
	serr = w.uploadOrStore()
	if serr != nil {
		if err != nil {
			err = fmt.Errorf("%v, previous error was: %v", serr, err)
		} else {
			err = serr
		}
	}
	w.closed = true
	return
}

// newWriter creates new writer according to the exchange given and returns it.
// If an error is reported, returned writer do not have to be closed.
func newWriter(exchange string, url string, directory string, alwaysDisk bool, logger *log.Logger) (w *writer, err error) {
	w = new(writer)
	w.exchange = exchange
	w.url = url
	w.directory = directory
	w.alwaysDisk = alwaysDisk
	w.sim, err = simulator.GetSimulator(exchange, nil)
	w.logger = logger
	return
}

// WriterQueueMethod is the enum-ized writer functions
type WriterQueueMethod int

const (
	// WriteMessage method
	WriteMessage = WriterQueueMethod(0)
	// WriteMessageChannelKnown method
	WriteMessageChannelKnown = WriterQueueMethod(1)
	// WriteSend method
	WriteSend = WriterQueueMethod(2)
	// WriteError method
	WriteError = WriterQueueMethod(3)
	// WriteEOS method
	WriteEOS = WriterQueueMethod(4)
)

// WriterQueueCapacity is the maximum capacity of the queue for a writer thread.
// If incoming message exceeded this capacity, then the caller would be blocked.
const WriterQueueCapacity = 10000

// WriterQueueElement is element of writer queue.
type WriterQueueElement struct {
	timestamp time.Time
	method    WriterQueueMethod
	channel   string
	message   []byte
}

// Writer is the concurrent version of writer.
type Writer struct {
	uw        *writer
	queue     chan *WriterQueueElement
	errc      chan error
	stop      chan struct{}
	closed    bool
	lastError error
	logger    *log.Logger
}

// writerRoutine houses a writer and accepts command for it.
// This is intended to be run on another goroutine.
// To stop writer thread, close queue channel.
func (w *Writer) writerRoutine() {
	var err error
	defer func() {
		if err != nil {
			w.errc <- err
		}
		close(w.errc)
	}()
	defer func() {
		serr := w.uw.Close(time.Now().UnixNano())
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("close: %v", serr)
			} else {
				err = fmt.Errorf("close: %v", serr)
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
			w.logger.Printf("%s", debug.Stack())
		}
	}()
	for {
		select {
		case elem := <-w.queue:
			var serr error
			switch elem.method {
			case WriteMessageChannelKnown:
				serr = w.uw.MessageChannelKnown(elem.channel, elem.timestamp.UnixNano(), elem.message)
			case WriteMessage:
				serr = w.uw.Message(elem.timestamp.UnixNano(), elem.message)
			case WriteSend:
				serr = w.uw.Send(elem.timestamp.UnixNano(), elem.message)
			case WriteError:
				serr = w.uw.Error(elem.timestamp.UnixNano(), elem.message)
			case WriteEOS:
				serr = w.uw.Close(elem.timestamp.UnixNano())
			default:
				serr = errors.New("unknown queue element method")
			}
			if serr != nil {
				// send error through channel and leave
				err = serr
				return
			}
		case <-w.stop:
			return
		}
	}
}

// Queue adds the given job on the writer queue.
// Blocks if the queue is overflown.
// Runs on the context given.
func (w *Writer) Queue(elem *WriterQueueElement) (err error) {
	if w.closed {
		return errors.New("writer: already closed")
	}
	if w.lastError != nil {
		return w.lastError
	}
	select {
	case w.queue <- elem:
		return nil
	case serr := <-w.errc:
		w.lastError = serr
		return serr
	}
}

// Close closes this writer and frees resources associated with.
// Blocks until the underlying writer routine stops.
// If already closed, call to this function returns the last error.
func (w *Writer) Close() error {
	if w.closed {
		return w.lastError
	}
	close(w.stop)
	w.closed = true
	// The error channel always will be closed
	serr, ok := <-w.errc
	if ok {
		return serr
	}
	return nil
}

// NewWriter creates and returns new concurrent writer.
// A writer routine can be stopped by closing `chan WriterQueueElement`.
func NewWriter(exchange string, urlStr string, directory string, alwaysDisk bool, logger *log.Logger, queueSize int) (w *Writer, err error) {
	w = new(Writer)
	w.logger = logger
	w.queue = make(chan *WriterQueueElement, queueSize)
	w.errc = make(chan error)
	w.stop = make(chan struct{})
	uw, serr := newWriter(exchange, urlStr, directory, alwaysDisk, logger)
	if serr != nil {
		err = serr
		return
	}
	w.uw = uw
	go w.writerRoutine()
	return
}
