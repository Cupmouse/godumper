package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sort"
	"strings"

	"github.com/exchangedataset/streamcommons/jsonstructs"
)

const bitfinexMaximumChannel = 30

var bitfinexPrioritySymbols = []string{"tBTCUSD", "tETHUSD"}

type bitfinexSubscriber struct {
	tradeSymbols []string
	logger       *log.Logger
}

type bitfinexTicker struct {
	Symbol              string
	Bid                 float64
	BidSize             float64
	Ask                 float64
	AskSize             float64
	DailyChange         float64
	DailyChangeRelative float64
	LastPrice           float64
	Volume              float64
	High                float64
	Low                 float64
	// this will be calculated
	USDVolume float64
}

type bitmexTickers []bitfinexTicker

func (a bitmexTickers) Len() int           { return len(a) }
func (a bitmexTickers) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a bitmexTickers) Less(i, j int) bool { return a[i].USDVolume < a[j].USDVolume }

func (s *bitfinexSubscriber) URL() string {
	return "wss://api-pub.bitfinex.com/ws/2"
}

func (s *bitfinexSubscriber) fetchMarketsSortedByUSDVolume() (symbols []string, err error) {
	// Before start dumping, bitfinex has too much currencies so it has channel limitation
	// We must cherry pick the best one to observe its trade
	// We can determine this by retrieving trading volumes for each symbol and pick coins which volume is in the most
	var res *http.Response
	res, err = http.Get("https://api.bitfinex.com/v2/tickers?symbols=ALL")
	if err != nil {
		return
	}
	defer func() {
		serr := res.Body.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("%v, original error was: %v", serr, err)
			} else {
				err = serr
			}
		}
	}()
	resBytes, serr := ioutil.ReadAll(res.Body)
	if serr != nil {
		return nil, serr
	}
	// This is to store tickers for all symbols including trade and funding
	tickersAll := make([]interface{}, 0, 1000)
	err = json.Unmarshal(resBytes, &tickersAll)
	if err != nil {
		return
	}
	// Take only normal exchange symbol which starts from 't', not funding symbol, 'f'
	// Symbol name is located at index 0
	tickersTrade := make(bitmexTickers, 0, 500)
	for _, tickerInterf := range tickersAll {
		ticker := tickerInterf.([]interface{})
		symbol := ticker[0].(string)
		// Ignore other than trade channels
		if !strings.HasPrefix(symbol, "t") {
			break
		}
		tickersTrade = append(tickersTrade, bitfinexTicker{
			Symbol:              ticker[0].(string),
			Bid:                 ticker[1].(float64),
			BidSize:             ticker[2].(float64),
			Ask:                 ticker[3].(float64),
			AskSize:             ticker[4].(float64),
			DailyChange:         ticker[5].(float64),
			DailyChangeRelative: ticker[6].(float64),
			LastPrice:           ticker[7].(float64),
			Volume:              ticker[8].(float64),
			High:                ticker[9].(float64),
			Low:                 ticker[10].(float64),
		})
	}
	// Volume is NOT in USD, example, tETHBTC volume is in BTC
	// Must convert it to USD in order to sort them by USD volume
	// For this, let's make a price table
	// Last price are located at index 7
	priceTable := make(map[string]float64)
	for _, ticker := range tickersTrade {
		priceTable[ticker.Symbol] = ticker.LastPrice
	}
	// Convert raw volume to USD volume
	// tXXXYYY (volume in XXX, price in YYY)
	// If tXXXUSD exist, then volume is (volume of tXXXYYY) * (price of tXXXUSD)
	for _, ticker := range tickersTrade {
		// take XXX of tXXXYYY
		pairBase := ticker.Symbol[1:4]
		usdPrice, ok := priceTable[fmt.Sprintf("t%sUSD", pairBase)]
		if ok {
			// (raw volume) * (usd price in usd) = volume in usd
			ticker.USDVolume = ticker.Volume * usdPrice
		} else {
			s.logger.Println("could not find proper market to calculate volume for symbol:", ticker.Symbol)
		}
	}
	// Sort slice by USD volume
	// Note it requires reverse option, since we are looking for symbols
	// Which have the most largest volume
	sort.Sort(sort.Reverse(tickersTrade))
	symbols = make([]string, len(tickersTrade))
	for i, ticker := range tickersTrade {
		symbols[i] = ticker.Symbol
	}
	return
}

func (s *bitfinexSubscriber) BeforeConnection() error {
	allMarkets, serr := s.fetchMarketsSortedByUSDVolume()
	if serr != nil {
		return serr
	}
	markets := append(bitfinexPrioritySymbols, allMarkets...)
	// Pick up certain amount of unique symbols from markets
	s.tradeSymbols = make([]string, bitfinexMaximumChannel/2)
	appeared := make(map[string]bool)
	i := 0
	for _, symbol := range markets {
		if i >= bitfinexMaximumChannel/2 {
			return nil
		}
		_, ok := appeared[symbol]
		if !ok {
			// unique market
			s.tradeSymbols[i] = symbol
			appeared[symbol] = true
			i++
		}
	}
	return nil
}

func (s *bitfinexSubscriber) AfterSubscribed() ([]queueElement, error) {
	return nil, nil
}

func (s *bitfinexSubscriber) Subscribe() ([][]byte, error) {
	subscribes := make([][]byte, 0, 100)
	subscribe := new(jsonstructs.BitfinexSubscribe)
	subscribe.Initialize()
	// Subscribe to trades channel
	subscribe.Channel = "trades"
	for _, symbol := range s.tradeSymbols {
		subscribe.Symbol = symbol
		marshaled, serr := json.Marshal(subscribe)
		if serr != nil {
			return nil, fmt.Errorf("subscribe marshal: %v", serr)
		}
		subscribes = append(subscribes, marshaled)
	}
	// Subscribe to book channel
	subscribe.Channel = "book"
	// Set precision to raw
	prec := "P0"
	subscribe.Prec = &prec
	// Set frequency to the most frequent == realtime
	frec := "F0"
	subscribe.Frec = &frec
	// Set limit to a big number
	len := "100"
	subscribe.Len = &len
	for _, symbol := range s.tradeSymbols {
		subscribe.Symbol = symbol
		marshaled, serr := json.Marshal(subscribe)
		if serr != nil {
			return nil, fmt.Errorf("subscribe marshal: %v", serr)
		}
		subscribes = append(subscribes, marshaled)
	}
	return subscribes, nil
}

func newBitfinexSubscriber(logger *log.Logger) (subber *bitfinexSubscriber) {
	subber = new(bitfinexSubscriber)
	subber.logger = logger
	return
}
