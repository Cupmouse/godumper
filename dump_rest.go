package main

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

// RestResponse is response from server via REST protocol.
type RestResponse struct {
	Request   *RestRequest
	Timestamp time.Time
	Body      []byte
}

// RestRequest is request to REST server with user defined metadata.
type RestRequest struct {
	URL      string
	Metadata interface{}
}

// RestClient is the client of http REST protocol.
type RestClient struct {
	requests  chan *RestRequest
	responses chan *RestResponse
	stop      chan struct{}
	errc      chan error
	closed    bool
	lastError error
}

func rest(ctx context.Context, urlString string) (b []byte, timestamp time.Time, err error) {
	req, serr := http.NewRequestWithContext(ctx, http.MethodGet, urlString, nil)
	if serr != nil {
		err = serr
		return
	}
	res, serr := http.DefaultClient.Do(req)
	if serr != nil {
		err = serr
		return
	}
	// For binance
	if res.StatusCode == http.StatusTooManyRequests || res.StatusCode == http.StatusTeapot {
		retryAfter, serr := strconv.Atoi(res.Header.Get("Retry-After"))
		if serr != nil {
			err = fmt.Errorf("retry-after Atoi: %v", serr)
			return
		}
		err = fmt.Errorf("need retry: got %v, wants to wait for %ds", res.StatusCode, retryAfter)
		return
	}
	timestamp = time.Now()
	defer func() {
		serr := res.Body.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("body close: %v, originally: %v", serr, err)
			} else {
				err = fmt.Errorf("body close: %v", serr)
			}
		}
	}()
	b, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}
	return
}

func (*RestClient) singleRestRoutine(ctx context.Context, rreq *RestRequest, reponse chan *RestResponse, errc chan error) {
	b, timestamp, serr := rest(ctx, rreq.URL)
	if serr != nil {
		errc <- serr
		return
	}
	reponse <- &RestResponse{
		Request:   rreq,
		Timestamp: timestamp,
		Body:      b,
	}
}

// To stop this routine, close `symbols`
func (c *RestClient) restRoutine() {
	var err error
	defer func() {
		if err != nil {
			c.errc <- err
		}
		close(c.errc)
	}()
	running := 0
	responses := make(chan *RestResponse)
	cerr := make(chan error)
	defer func() {
		for running > 0 {
			// Sub routine will exit either by sending an body or an error
			select {
			case <-responses:
			case serr := <-cerr:
				if err != nil {
					err = fmt.Errorf("rest: %v, originally: %v", serr, err)
				} else {
					err = serr
				}
			}
			running--
		}
		close(cerr)
		close(responses)
		// Have to close c.responses ealier than c.errc, this is used by others to check if an error is present
		close(c.responses)
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		select {
		case req := <-c.requests:
			// New job
			go c.singleRestRoutine(ctx, req, responses, cerr)
			running++
		case res := <-responses:
			running--
			sent := false
			for !sent {
				select {
				case c.responses <- res:
					sent = true
				case req := <-c.requests:
					go c.singleRestRoutine(ctx, req, responses, cerr)
					running++
				case err = <-cerr:
					running--
					return
				case <-c.stop:
					return
				}
			}
		case err = <-cerr:
			running--
			return
		case <-c.stop:
			return
		}
	}
}

// Request adds a request to the queue.
// Blocks if the queue is full.
// Returns immidiately if a client is closed
func (c *RestClient) Request(req *RestRequest) (err error) {
	if c.lastError != nil {
		// Report the last error
		return c.lastError
	}
	if c.closed {
		return errors.New("rest: already closed")
	}
	select {
	case c.requests <- req:
		// Request sent
	case err = <-c.errc:
		c.lastError = err
		return
	}
	return
}

// Response returns the response channel.
// If the channel was closed, then check the latest error using `Error`.
func (c *RestClient) Response() chan *RestResponse {
	return c.responses
}

// Error returns the last error.
// Blocks until an error is avaiable, returns immidiately if the latest error is avaliable.
// Always returns an error if the client is closed.
func (c *RestClient) Error() (err error) {
	if c.closed {
		return errors.New("rest: already closed")
	}
	serr, ok := <-c.errc
	if ok {
		c.lastError = serr
		return serr
	}
	return c.lastError
}

// Close closes this client and frees resorces associated with.
// If the client has been already closed, then returns the latest error.
func (c *RestClient) Close() error {
	if c.closed {
		return c.lastError
	}
	// This will stop routines
	close(c.stop)
	c.closed = true
	// Wait for the controller routine to stop
	serr, ok := <-c.errc
	if ok {
		c.lastError = serr
		return serr
	}
	return nil
}

// NewRestClient creates a new rest client.
func NewRestClient() *RestClient {
	c := new(RestClient)
	c.errc = make(chan error)
	c.requests = make(chan *RestRequest)
	c.responses = make(chan *RestResponse)
	c.stop = make(chan struct{})
	go c.restRoutine()
	return c
}
