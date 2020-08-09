package main

import (
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

const binanceMaximumDepthSubscribe = 1

// const binanceSymbol

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

func (s *binanceSubscriber) URL() string {
	return "wss://stream.binance.com:9443/stream"
}

func binanceREST(url string, query url.Values) (body []byte, err error) {
	req, serr := http.NewRequest(http.MethodGet, url, nil)
	if serr != nil {
		return nil, serr
	}
	if query != nil {
		req.URL.RawQuery = query.Encode()
	}
	res, serr := http.DefaultClient.Do(req)
	if serr != nil {
		return nil, serr
	}
	defer func() {
		serr := res.Body.Close()
		if serr != nil {
			if err != nil {
				err = serr
			} else {
				err = fmt.Errorf("%v, original: %v", serr, err)
			}
		}
	}()
	if res.StatusCode == http.StatusTooManyRequests || res.StatusCode == http.StatusTeapot {
		retryAfter, serr := strconv.Atoi(res.Header.Get("Retry-After"))
		if serr != nil {
			return nil, serr
		}
		time.Sleep(time.Duration(retryAfter) * time.Second)
		// This will generated error on subscriber thread
		return nil, fmt.Errorf("need retry: waited %ds for %d", retryAfter, res.StatusCode)
	}
	return ioutil.ReadAll(res.Body)
}

func (s *binanceSubscriber) BeforeConnection() error {
	tickerBody, serr := binanceREST("https://www.binance.com/api/v3/ticker/24hr", nil)
	tickers := make([]jsonstructs.BinanceTicker24HrElement, 0, 5000)
	serr = json.Unmarshal(tickerBody, &tickers)
	if serr != nil {
		return fmt.Errorf("ticker 24hr: %v", serr)
	}
	exchangeInfoBody, serr := binanceREST("https://www.binance.com/api/v3/exchangeInfo", nil)
	if serr != nil {
		return fmt.Errorf("exchangeInfo: %v", serr)
	}
	exchangeInfo := new(jsonstructs.BinanceExchangeInfo)
	serr = json.Unmarshal(exchangeInfoBody, &exchangeInfo)
	if serr != nil {
		return fmt.Errorf("exchangeInfo unmarshal: %v", serr)
	}
	// Calculate USDT volume
	// Make price table for all pairs
	priceTable := make(map[string]float64)
	for _, ticker := range tickers {
		lastPrice, serr := strconv.ParseFloat(ticker.LastPrice, 64)
		if serr != nil {
			return fmt.Errorf("LastPrice ParseFloat: %v", serr)
		}
		priceTable[ticker.Symbol] = lastPrice
	}
	// Calculate USD volume of pairs using its base currency vs its USDT market
	usdVolumes := make(binanceUSDTVolumePairs, len(tickers))
	// exchangeInfo.Symbols and tickers share the same index
	for i, pair := range exchangeInfo.Symbols {
		ticker := tickers[i]
		if ticker.Symbol != pair.Symbol {
			return errors.New("ticker.Symbol != pair.Symbol")
		}
		var usdVolume float64
		if pair.QuoteAsset == "USD" || pair.QuoteAsset == "USDT" {
			// Volume is already in USD
			var serr error
			usdVolume, serr = strconv.ParseFloat(ticker.QuoteVolume, 64)
			if serr != nil {
				return fmt.Errorf("QuoteVolume ParseFloat: %v", serr)
			}
		} else {
			// Calculate usd volume from a base vs USDT market
			baseUSDPrice, ok := priceTable[pair.BaseAsset+"USDT"]
			if ok {
				volume, serr := strconv.ParseFloat(ticker.Volume, 64)
				if serr != nil {
					return fmt.Errorf("Volume ParseFloat: %v", serr)
				}
				usdVolume = baseUSDPrice * volume
			} else {
				// Calculate USD volume from a quote vs USDT market
				quoteUSDPrice, ok := priceTable[pair.QuoteAsset+"USDT"]
				if ok {
					quoteVolume, serr := strconv.ParseFloat(ticker.QuoteVolume, 64)
					if serr != nil {
						return fmt.Errorf("QuoteVolume ParseFloat: %v", serr)
					}
					usdVolume = quoteUSDPrice * quoteVolume
				} else {
					s.logger.Printf("no usdt market for %v\n", pair.Symbol)
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
	s.largeVolumeSymbols = make([]string, size)
	for i := 0; i < size; i++ {
		s.largeVolumeSymbols[i] = usdVolumes[i].Symbol
	}
	s.logger.Println("List of symbols selected:", s.largeVolumeSymbols)
	return nil
}

func (s *binanceSubscriber) AfterSubscribed() (msgs []queueElement, err error) {
	msgs = make([]queueElement, len(s.largeVolumeSymbols))
	for i, symbol := range s.largeVolumeSymbols {
		// Set query
		query := make(url.Values)
		query.Set("symbol", symbol)
		query.Set("limit", "1000")
		body, serr := binanceREST("https://www.binance.com/api/v3/depth", query)
		if serr != nil {
			return nil, serr
		}
		now := time.Now()
		msgs[i] = queueElement{
			method:    MessageChannelKnown,
			channel:   strings.ToLower(symbol) + "@" + streamcommons.BinanceStreamRESTDepth,
			timestamp: now,
			message:   body,
		}
	}
	return
}

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

func (s *binanceSubscriber) Subscribe() ([][]byte, error) {
	subscribes := make([][]byte, 3*len(s.largeVolumeSymbols))
	// ID must not be zero as it's used as a flag to check if a unmarshaling response were
	// successful
	id := 1
	i := 0
	for _, symbol := range s.largeVolumeSymbols {
		lowerSymbol := strings.ToLower(symbol)
		dsub, serr := binanceGenerateSubscribe(id, lowerSymbol+"@depth@100ms")
		id++
		if serr != nil {
			return nil, fmt.Errorf("depth: %v", serr)
		}
		subscribes[i] = dsub
		i++
		tsub, serr := binanceGenerateSubscribe(id, lowerSymbol+"@trade")
		id++
		if serr != nil {
			return nil, fmt.Errorf("trade: %v", serr)
		}
		subscribes[i] = tsub
		i++
		tisub, serr := binanceGenerateSubscribe(id, lowerSymbol+"@ticker")
		id++
		if serr != nil {
			return nil, fmt.Errorf("trade: %v", serr)
		}
		subscribes[i] = tisub
		i++
	}
	return subscribes, nil
}

func newBinanceSubscriber(logger *log.Logger) (subber *binanceSubscriber) {
	subber = new(binanceSubscriber)
	subber.logger = logger
	return
}
