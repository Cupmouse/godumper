package writer

import (
	"compress/gzip"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"github.com/exchangedataset/streamcommons/simulator"
)

// Writer writes messages to gzipped file
type Writer struct {
	closed        bool
	lastTimestamp int64
	lock          sync.Mutex
	file          *os.File
	gwriter       *gzip.Writer
	exchange      string
	url           string
	directory     string
	logger        *log.Logger
	sim           simulator.Simulator
}

func (w *Writer) open(timestamp int64) (correctedTimestamp int64, err error) {
	if w.closed {
		err = errors.New("tried to open already closed writer")
		return
	}

	// correct time from going backwards
	if timestamp < w.lastTimestamp {
		// time is running backwards
		// probably because of system time correction
		w.logger.Println("timestamp is older than the last observed, substituting it to the last observed value")
		timestamp = w.lastTimestamp
	}
	correctedTimestamp = timestamp

	// it creates new file for every minute
	minute := int64(time.Duration(timestamp) / time.Minute)
	lastMinute := int64(time.Duration(w.lastTimestamp) / time.Minute)

	if w.file != nil && minute == lastMinute {
		// continues to use the same stream & file name
		return
	}

	// new name for file would be <exchange>_<timestamp>.gz
	fileName := fmt.Sprintf("%s_%d.gz", w.exchange, timestamp)
	filePath := path.Join(w.directory, fileName)

	w.logger.Printf("making new file: %s\n", fileName)

	isFirstFile := w.file == nil

	// change file name
	// close file and gzip writer if previous one exist
	if !isFirstFile {
		err = w.close()
		if err != nil {
			return
		}
	}

	// make directories to store file
	os.MkdirAll(w.directory, 0744)
	// open new file
	w.file, err = os.Create(filePath)
	// prepare gzip writer
	w.gwriter = gzip.NewWriter(w.file)

	if isFirstFile {
		// write start line
		startLine := fmt.Sprintf("start\t%d\t%s\n", timestamp, w.url)
		_, err = w.gwriter.Write([]byte(startLine))
		if err != nil {
			return
		}
	} else if minute%10 == 0 {
		// if last digit of minute is 0 then write state snapshot
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
	// set given timestamp as last write time
	w.lastTimestamp = timestamp

	return
}

// close closes file and gzip writer but it won't lock w.lock and also does not mark it as closed
func (w *Writer) close() (err error) {
	serr := w.gwriter.Flush()
	if serr != nil {
		err = fmt.Errorf("error on flushing gzip: %v", serr)
	}
	serr = w.gwriter.Close()
	if serr != nil {
		if err != nil {
			err = fmt.Errorf("error on closing gzip: %v, previous error was: %v", serr, err)
		} else {
			err = fmt.Errorf("error on closing gzip: %v", serr)
		}
	}
	serr = w.file.Close()
	if serr != nil {
		if err != nil {
			err = fmt.Errorf("error on closing file: %v, previous error was: %v", serr, err)
		} else {
			err = fmt.Errorf("error on closing file: %v", serr)
		}
	}
	return
}

// Message writes msg line to writer. Channel is automatically determined.
func (w *Writer) Message(timestamp int64, message []byte) (err error) {
	// mark this writer is locked so routines in other thread will wait
	w.lock.Lock()
	defer func() {
		w.lock.Unlock()
	}()
	timestamp, err = w.open(timestamp)
	if err != nil {
		return
	}
	var channel string
	channel, err = w.sim.ProcessMessage(message)
	if channel == "" || channel == simulator.ChannelUnknown {
		// simulator could not determine the channel of message
		w.logger.Println("channel is unknown:", string(message))
	}
	// write message despite the error (if happened)
	w.gwriter.Write([]byte(fmt.Sprintf("msg\t%d\t%s\t", timestamp, channel)))
	w.gwriter.Write(message)
	w.gwriter.Write([]byte("\n"))
	return
}

// Send writes send line to writer. Channel is automatically determined.
func (w *Writer) Send(timestamp int64, message []byte) (err error) {
	w.lock.Lock()
	defer func() {
		w.lock.Unlock()
	}()
	timestamp, err = w.open(timestamp)
	if err != nil {
		return
	}
	var channel string
	channel, err = w.sim.ProcessSend(message)
	if channel == "" || channel == simulator.ChannelUnknown {
		// simulator could not determine the channel of message
		w.logger.Println("channel is unknown:", string(message))
	}
	w.gwriter.Write([]byte(fmt.Sprintf("send\t%d\t%s\t", timestamp, channel)))
	w.gwriter.Write(message)
	w.gwriter.Write([]byte("\n"))
	return
}

// Error writes err line to writer.
func (w *Writer) Error(timestamp int64, message []byte) (err error) {
	w.lock.Lock()
	defer func() {
		w.lock.Unlock()
	}()
	timestamp, err = w.open(timestamp)
	if err != nil {
		return
	}
	w.gwriter.Write([]byte(fmt.Sprintf("err\t%d\t%s\t\n", timestamp, message)))
	return
}

// Close closes this writer and underlying file and gzip writer. It also writes eos line.
func (w *Writer) Close(timestamp int64) (err error) {
	w.lock.Lock()
	defer func() {
		w.lock.Unlock()
	}()
	// if already closed, raise error
	if w.closed {
		err = errors.New("writer is already closed, can not close again")
		return
	}
	timestamp, err = w.open(timestamp)
	if err != nil {
		return
	}
	w.gwriter.Write([]byte(fmt.Sprintf("eos\t%d\n", timestamp)))
	// report error as it is
	err = w.close()
	w.closed = true
	return
}

// NewWriter creates new writer according to exchange given and returns it
// if error is reported, then there is no need to close returned writer
func NewWriter(exchange string, url string, directory string, logger *log.Logger) (w *Writer, err error) {
	w = new(Writer)
	w.exchange = exchange
	w.url = url
	w.directory = directory
	w.sim, err = simulator.GetSimulator(exchange, nil)
	w.logger = logger
	return
}
