package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/exchangedataset/streamcommons"
	"github.com/exchangedataset/streamcommons/jsonstructs"
)

const binanceMaximumDepthSubscribe = 20

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

func binanceFetchLargestVolumeSymbols(ctx context.Context, logger *log.Logger) ([]string, error) {
	tickerBody, _, serr := rest(ctx, "https://www.binance.com/api/v3/ticker/24hr")
	tickers := make([]jsonstructs.BinanceTicker24HrElement, 0, 5000)
	serr = json.Unmarshal(tickerBody, &tickers)
	if serr != nil {
		return nil, fmt.Errorf("ticker 24hr: %v", serr)
	}
	exchangeInfoBody, _, serr := rest(ctx, "https://www.binance.com/api/v3/exchangeInfo")
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
			return nil, fmt.Errorf("LastPrice: %v", serr)
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
				return nil, fmt.Errorf("QuoteVolume: %v", serr)
			}
		} else {
			// Calculate usd volume from a base vs USDT market
			baseUSDPrice, ok := priceTable[pair.BaseAsset+"USDT"]
			if ok {
				volume, serr := strconv.ParseFloat(ticker.Volume, 64)
				if serr != nil {
					return nil, fmt.Errorf("Volume: %v", serr)
				}
				usdVolume = baseUSDPrice * volume
			} else {
				// Calculate USD volume from a quote vs USDT market
				quoteUSDPrice, ok := priceTable[pair.QuoteAsset+"USDT"]
				if ok {
					quoteVolume, serr := strconv.ParseFloat(ticker.QuoteVolume, 64)
					if serr != nil {
						return nil, fmt.Errorf("QuoteVolume: %v", serr)
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

func dumpBinance(ctx context.Context, directory string, alwaysDisk bool, logger *log.Logger) (err error) {
	// NOTE: symbols are in lower-case!
	symbols, serr := binanceFetchLargestVolumeSymbols(ctx, logger)
	if serr != nil {
		return fmt.Errorf("market fetch: %v", serr)
	}
	logger.Println("List of symbols selected:", symbols)
	streams := make([]string, 3*len(symbols))
	for i, symbol := range symbols {
		streams[i*3+0] = symbol + "@depth@100ms"
		streams[i*3+1] = symbol + "@trade"
		streams[i*3+2] = symbol + "@ticker"
	}
	u, serr := url.Parse("wss://stream.binance.com:9443/stream?streams=" + strings.Join(streams, "/"))
	if serr != nil {
		return serr
	}
	us := u.String()
	writer, serr := NewWriter("binance", us, directory, alwaysDisk, logger, 10000)
	if serr != nil {
		return serr
	}
	defer func() {
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
		return serr
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
	rest := NewRestClient()
	defer func() {
		serr := rest.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("%v, originally: %v", serr, err)
			} else {
				err = serr
			}
		}
	}()
	logger.Println("binance dumping")
	// Map of depth symbols who needs rest message
	restNeeded := make(map[string]bool)
	for _, symbol := range symbols {
		restNeeded[symbol] = true
	}
	for {
		select {
		case event, ok := <-ws.Event():
			if !ok {
				// Check for error
				return ws.Error()
			}
			var m WriterQueueMethod
			switch event.Type {
			case WebSocketEventReceive:
				m = WriteMessage
			case WebSocketEventSend:
				m = WriteSend
			}
			// Record message
			err = writer.Queue(&WriterQueueElement{
				method:    m,
				timestamp: event.Timestamp,
				message:   event.Message,
			})
			if err != nil {
				return
			}
			if event.Type != WebSocketEventReceive {
				continue
			}
			if len(restNeeded) == 0 {
				continue
			}
			// Check if this channel is depth
			// If depth, check if it needs a rest message
			root := new(jsonstructs.BinanceReponseRoot)
			serr := json.Unmarshal(event.Message, root)
			if serr != nil {
				return fmt.Errorf("root unmarshal: %v", serr)
			}
			if root.Stream == "" {
				continue
			}
			symbol, stream, serr := streamcommons.BinanceDecomposeChannel(root.Stream)
			if serr != nil {
				err = serr
				return
			}
			if stream != "depth@100ms" {
				continue
			}
			_, ok = restNeeded[symbol]
			if !ok {
				continue
			}
			// A REST message is needed
			query := make(url.Values)
			query.Set("symbol", strings.ToUpper(symbol))
			query.Set("limit", "1000")
			u, serr := url.Parse("https://www.binance.com/api/v3/depth")
			if serr != nil {
				err = serr
				return
			}
			u.RawQuery = query.Encode()
			err = rest.Request(&RestRequest{
				URL:      u.String(),
				Metadata: strings.ToLower(symbol),
			})
			if err != nil {
				return
			}
			// Don't need a REST message anymore
			delete(restNeeded, symbol)
		case rres, ok := <-rest.Response():
			if !ok {
				return rest.Error()
			}
			err = writer.Queue(&WriterQueueElement{
				method:    WriteMessageChannelKnown,
				channel:   rres.Request.Metadata.(string) + "@" + streamcommons.BinanceStreamRESTDepth,
				timestamp: rres.Timestamp,
				message:   rres.Body,
			})
			if err != nil {
				return
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
