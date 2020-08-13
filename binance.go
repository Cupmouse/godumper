package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/exchangedataset/streamcommons"
	"github.com/exchangedataset/streamcommons/jsonstructs"
)

const binanceMaximumDepthSubscribe = 5

var binancePrioritySymbols = []string{"BTCUSDT", "tETHUSD"}

type binanceSubscriber struct {
	depth jsonstructs.BinanceDepthREST
	// Symbols are in upper-case
	largeVolumeSymbols []string
	logger             *log.Logger
}

type binanceUSDTVolumePairs []struct {
	Symbol string
	Volume float64
}

func (a binanceUSDTVolumePairs) Len() int           { return len(a) }
func (a binanceUSDTVolumePairs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a binanceUSDTVolumePairs) Less(i, j int) bool { return a[i].Volume < a[j].Volume }

func binanceGenerateSubscribe(id int, channel string) ([]byte, error) {
	subscribe := new(jsonstructs.BinanceSubscribe)
	subscribe.Initialize()
	subscribe.ID = id
	subscribe.Params = []string{channel}
	msub, serr := json.Marshal(subscribe)
	if serr != nil {
		return nil, fmt.Errorf("subscribe marshal: %v", serr)
	}
	return msub, nil
}

func binanceRESTSequential(ctx context.Context, ts string, query url.Values) ([]byte, error) {
	req, serr := http.NewRequestWithContext(ctx, http.MethodGet, ts, nil)
	if serr != nil {
		return nil, serr
	}
	req.URL.RawQuery = query.Encode()
	res, serr := http.DefaultClient.Do(req)
	if serr != nil {
		return nil, serr
	}
	if res.StatusCode == http.StatusTooManyRequests || res.StatusCode == http.StatusTeapot {
		retryAfter, serr := strconv.Atoi(res.Header.Get("Retry-After"))
		if serr != nil {
			return nil, serr
		}
		// retryAfter is frequentry 0, but active for a sub-second?
		time.Sleep(time.Duration(retryAfter+1) * time.Second)
		// This will generated error on subscriber thread
		return nil, fmt.Errorf("need retry: waited %ds for %d", retryAfter, res.StatusCode)
	}
	return ioutil.ReadAll(res.Body)
}

func binanceFetchLargestVolumeSymbols(ctx context.Context, logger *log.Logger) ([]string, error) {
	tickerBody, serr := binanceRESTSequential(ctx, "https://www.binance.com/api/v3/ticker/24hr", nil)
	tickers := make([]jsonstructs.BinanceTicker24HrElement, 0, 5000)
	serr = json.Unmarshal(tickerBody, &tickers)
	if serr != nil {
		return nil, fmt.Errorf("ticker 24hr: %v", serr)
	}
	exchangeInfoBody, serr := binanceRESTSequential(ctx, "https://www.binance.com/api/v3/exchangeInfo", nil)
	if serr != nil {
		return nil, fmt.Errorf("exchangeInfo: %v", serr)
	}
	exchangeInfo := new(jsonstructs.BinanceExchangeInfo)
	serr = json.Unmarshal(exchangeInfoBody, &exchangeInfo)
	if serr != nil {
		return nil, fmt.Errorf("exchangeInfo unmarshal: %v", serr)
	}
	// Calculate USDT volume
	// Make price table for all pairs
	priceTable := make(map[string]float64)
	for _, ticker := range tickers {
		lastPrice, serr := strconv.ParseFloat(ticker.LastPrice, 64)
		if serr != nil {
			return nil, fmt.Errorf("LastPrice ParseFloat: %v", serr)
		}
		priceTable[ticker.Symbol] = lastPrice
	}
	// Calculate USD volume of pairs using its base currency vs its USDT market
	usdVolumes := make(binanceUSDTVolumePairs, len(tickers))
	// exchangeInfo.Symbols and tickers share the same index
	for i, pair := range exchangeInfo.Symbols {
		ticker := tickers[i]
		if ticker.Symbol != pair.Symbol {
			return nil, errors.New("ticker.Symbol != pair.Symbol")
		}
		var usdVolume float64
		if pair.QuoteAsset == "USD" || pair.QuoteAsset == "USDT" {
			// Volume is already in USD
			var serr error
			usdVolume, serr = strconv.ParseFloat(ticker.QuoteVolume, 64)
			if serr != nil {
				return nil, fmt.Errorf("QuoteVolume ParseFloat: %v", serr)
			}
		} else {
			// Calculate usd volume from a base vs USDT market
			baseUSDPrice, ok := priceTable[pair.BaseAsset+"USDT"]
			if ok {
				volume, serr := strconv.ParseFloat(ticker.Volume, 64)
				if serr != nil {
					return nil, fmt.Errorf("Volume ParseFloat: %v", serr)
				}
				usdVolume = baseUSDPrice * volume
			} else {
				// Calculate USD volume from a quote vs USDT market
				quoteUSDPrice, ok := priceTable[pair.QuoteAsset+"USDT"]
				if ok {
					quoteVolume, serr := strconv.ParseFloat(ticker.QuoteVolume, 64)
					if serr != nil {
						return nil, fmt.Errorf("QuoteVolume ParseFloat: %v", serr)
					}
					usdVolume = quoteUSDPrice * quoteVolume
				} else {
					logger.Printf("no usdt market for %v\n", pair.Symbol)
					usdVolume = 0
				}
			}
		}
		usdVolumes[i].Symbol = pair.Symbol
		usdVolumes[i].Volume = usdVolume
	}
	// Sort on usd volume descend
	sort.Sort(sort.Reverse(usdVolumes))
	// Store some of pairs that have the largest USD volume
	var size int
	if len(usdVolumes) < binanceMaximumDepthSubscribe {
		size = len(usdVolumes)
	} else {
		size = binanceMaximumDepthSubscribe
	}
	largeVolumeSymbols := make([]string, size)
	for i := 0; i < size; i++ {
		largeVolumeSymbols[i] = strings.ToLower(usdVolumes[i].Symbol)
	}
	return largeVolumeSymbols, nil
}

func binanceRESTDepth(ctx context.Context, symbol string, body chan WriterQueueElement, errc chan error) {
	var err error
	defer func() {
		if err != nil {
			errc <- err
		}
	}()
	query := make(url.Values)
	query.Set("symbol", symbol)
	query.Set("limit", "1000")
	b, serr := binanceRESTSequential(ctx, "https://www.binance.com/api/v3/depth", query)
	if serr != nil {
		err = serr
		return
	}
	body <- WriterQueueElement{
		method:    WriteMessageChannelKnown,
		channel:   strings.ToLower(symbol) + "@" + streamcommons.BinanceStreamRESTDepth,
		timestamp: time.Now(),
		message:   b,
	}
}

// To stop this routine, close `symbols`
func binanceRESTRoutine(symbols chan string, out chan WriterQueueElement, errc chan error) {
	var err error
	defer func() {
		if err != nil {
			errc <- err
		}
		close(errc)
		close(out)
	}()
	running := 0
	ctx, cancel := context.WithCancel(context.Background())
	body := make(chan WriterQueueElement)
	rerr := make(chan error)
	defer func() {
		// This will stop the sub-routine
		cancel()
		for running > 0 {
			// Sub routine will exit either by sending an body or an error
			select {
			case <-body:
			case <-rerr:
			}
			running--
		}
		close(rerr)
		close(body)
	}()
	for {
		select {
		case symbol, ok := <-symbols:
			if !ok {
				// Stop
				return
			}
			go binanceRESTDepth(ctx, strings.ToUpper(symbol), body, rerr)
			running++
		case b := <-body:
			running--
			sent := false
			for !sent {
				select {
				case out <- b:
					sent = true
				case symbol, ok := <-symbols:
					if !ok {
						return
					}
					go binanceRESTDepth(ctx, strings.ToUpper(symbol), body, rerr)
					running++
				}
			}
		case err = <-rerr:
			running--
			return
		}
	}
}

func dumpBinance(directory string, alwaysDisk bool, logger *log.Logger, stop chan struct{}, errc chan error) {
	defer close(errc)
	var err error
	defer func() {
		if err != nil {
			errc <- err
		}
	}()
	// NOTE: symbols are in lower-case!
	symbols, serr := binanceFetchLargestVolumeSymbols(context.Background(), logger)
	if serr != nil {
		err = fmt.Errorf("market fetch: %v", serr)
		return
	}
	logger.Println("List of symbols selected:", symbols)
	us := "wss://stream.binance.com:9443/stream"
	writer, werr := NewWriter("binance", us, directory, alwaysDisk, logger, 10000)
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
	subscribes := make([][]byte, 3*len(symbols))
	// ID must not be zero as it's used as a flag to check if a unmarshaling response were
	// successful
	id := 1
	i := 0
	for _, symbol := range symbols {
		dsub, serr := binanceGenerateSubscribe(id, symbol+"@depth@100ms")
		id++
		if serr != nil {
			err = fmt.Errorf("depth: %v", serr)
			break
		}
		subscribes[i] = dsub
		i++
		tsub, serr := binanceGenerateSubscribe(id, symbol+"@trade")
		id++
		if serr != nil {
			err = fmt.Errorf("trade: %v", serr)
			break
		}
		subscribes[i] = tsub
		i++
		tisub, serr := binanceGenerateSubscribe(id, symbol+"@ticker")
		id++
		if serr != nil {
			err = fmt.Errorf("trade: %v", serr)
			break
		}
		subscribes[i] = tisub
		i++
	}
	if err != nil {
		err = fmt.Errorf("subscribe: %v", err)
		return
	}
	for _, sub := range subscribes {
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
	logger.Println("binance dumping")
	restJob := make(chan string)
	rest := make(chan WriterQueueElement)
	rerr := make(chan error)
	go binanceRESTRoutine(restJob, rest, rerr)
	defer func() {
		close(restJob)
		serr, ok := <-rerr
		if ok {
			if err != nil {
				err = fmt.Errorf("rest: %v, originally: %v", serr, err)
			} else {
				err = fmt.Errorf("rest: %v", serr)
			}
		}
	}()
	// Map of depth symbols who needs rest message
	restNeeded := make(map[string]bool)
	for _, symbol := range symbols {
		restNeeded[symbol] = true
	}
	for {
		select {
		case e := <-event:
			var m WriterQueueMethod
			switch e.Type {
			case WebSocketEventReceive:
				m = WriteMessage
				if len(restNeeded) > 0 {
					// Check if this channel is depth
					// If depth, check if it needs a rest message
					root := new(jsonstructs.BinanceReponseRoot)
					serr := json.Unmarshal(e.Message, root)
					if serr != nil {
						err = fmt.Errorf("root unmarshal: %v", serr)
						return
					}
					if root.Stream != "" {
						symbol, stream, serr := streamcommons.BinanceDecomposeChannel(root.Stream)
						if serr != nil {
							err = serr
							return
						}
						if stream == "depth@100ms" {
							_, ok := restNeeded[symbol]
							if ok {
								// A REST message is needed
								select {
								case restJob <- symbol:
								case serr := <-rerr:
									err = fmt.Errorf("rest: %v", serr)
									return
								case <-stop:
									return
								case rb := <-rest:
									select {
									case writer <- rb:
									case serr := <-werr:
										err = fmt.Errorf("writer: %v", serr)
										return
									case <-stop:
										return
									}
								}
								// Don't need a REST message anymore
								delete(restNeeded, symbol)
							}
						}
					}
				}
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
		case rb := <-rest:
			select {
			case writer <- rb:
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
		case serr := <-rerr:
			err = fmt.Errorf("rest: %v", serr)
			return
		case <-stop:
			// Stop signal received
			return
		}
	}
}
