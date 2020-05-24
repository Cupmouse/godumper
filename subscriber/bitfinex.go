package subscriber

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

type bitfinexTickerTrade struct {
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

type bitmexTickerTrades []bitfinexTickerTrade

func (a bitmexTickerTrades) Len() int           { return len(a) }
func (a bitmexTickerTrades) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a bitmexTickerTrades) Less(i, j int) bool { return a[i].USDVolume < a[j].USDVolume }

func (d *bitfinexSubscriber) URL() string {
	return "wss://api-pub.bitfinex.com/ws/2"
}

func (d *bitfinexSubscriber) fetchMarketsSortedByUSDVolume() (symbols []string, err error) {
	// before start dumping, bitfinex has too much currencies so it has channel limitation
	// we must cherry pick the best one to observe its trade
	// we can determine this by retrieving trading volumes for each symbol and pick coins which volume is in the most
	var res *http.Response
	res, err = http.Get("https://api.bitfinex.com/v2/tickers?symbols=ALL")
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
	if err != nil {
		return
	}

	var resBytes []byte
	resBytes, err = ioutil.ReadAll(res.Body)

	// this is to store tickers for all symbols including trade and funding
	tickersAll := make([]interface{}, 0, 1000)
	err = json.Unmarshal(resBytes, &tickersAll)
	if err != nil {
		return
	}

	// take only normal exchange symbol which starts from 't', not funding symbol, 'f'
	// symbol name is located at index 0
	tickersTrade := make(bitmexTickerTrades, 0, 500)
	for _, tickerInterf := range tickersAll {
		ticker := tickerInterf.([]interface{})
		symbol := ticker[0].(string)

		// ignore other than trade channels
		if !strings.HasPrefix(symbol, "t") {
			break
		}

		tickersTrade = append(tickersTrade, bitfinexTickerTrade{
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

	// volume is NOT in USD, example, tETHBTC volume is in BTC
	// must convert it to USD in order to sort them by USD volume
	// for this, let's make a price table
	// last price are located at index 7
	priceTable := make(map[string]float64)
	for _, ticker := range tickersTrade {
		priceTable[ticker.Symbol] = ticker.LastPrice
	}

	// convert raw volume to USD volume
	// tXXXYYY (volume in XXX, price in YYY)
	// if tXXXUSD exist, then volume is (volume of tXXXYYY) * (price of tXXXUSD)
	for _, ticker := range tickersTrade {
		// take XXX of tXXXYYY
		pairBase := ticker.Symbol[1:4]

		usdPrice, ok := priceTable[fmt.Sprintf("t%sUSD", pairBase)]
		if ok {
			// (raw volume) * (usd price in usd) = volume in usd
			ticker.USDVolume = ticker.Volume * usdPrice
		} else {
			d.logger.Println("could not find proper market to calculate volume for symbol:", ticker.Symbol)
		}
	}

	// sort slice by USD volume
	// note it requires reverse option, since we are looking for symbols
	// which have the most largest volume
	sort.Sort(sort.Reverse(tickersTrade))

	symbols = make([]string, len(tickersTrade))
	for i, ticker := range tickersTrade {
		symbols[i] = ticker.Symbol
	}
	return
}

func (d *bitfinexSubscriber) BeforeConnection() (err error) {
	var allMarkets []string
	allMarkets, err = d.fetchMarketsSortedByUSDVolume()
	if err != nil {
		return
	}

	markets := append(bitfinexPrioritySymbols, allMarkets...)

	// pick up certain amount of unique symbols from markets
	d.tradeSymbols = make([]string, bitfinexMaximumChannel/2)
	appeared := make(map[string]bool)
	i := 0
	for _, symbol := range markets {
		if i >= bitfinexMaximumChannel/2 {
			return
		}
		_, ok := appeared[symbol]
		if !ok {
			// unique market
			d.tradeSymbols[i] = symbol
			appeared[symbol] = true
			i++
		}
	}
	return
}

func (d *bitfinexSubscriber) Subscribe() (subscribes []Subscribe, err error) {
	subscribes = make([]Subscribe, 0, 100)

	subscribe := new(jsonstructs.BitfinexSubscribe)
	subscribe.Initialize()

	// Subscribe to trades channel
	subscribe.Channel = "trades"

	for _, symbol := range d.tradeSymbols {
		subscribe.Symbol = symbol
		var marshaled []byte
		marshaled, err = json.Marshal(subscribe)
		if err != nil {
			return
		}
		channel := fmt.Sprintf("%s_%s", subscribe.Channel, symbol)
		subscribes = append(subscribes, Subscribe{Channel: channel, Message: marshaled})
	}

	// subscribe to book channel
	subscribe.Channel = "book"
	// set precision to raw
	prec := "P0"
	subscribe.Prec = &prec
	// set frequency to the most frequent == realtime
	frec := "F0"
	subscribe.Frec = &frec
	// set limit to a big number
	len := "100"
	subscribe.Len = &len

	for _, symbol := range d.tradeSymbols {
		subscribe.Symbol = symbol
		channel := fmt.Sprintf("%s_%s", subscribe.Channel, symbol)
		var marshaled []byte
		marshaled, err = json.Marshal(subscribe)
		if err != nil {
			return
		}
		subscribes = append(subscribes, Subscribe{Channel: channel, Message: marshaled})
	}
	return
}

func newBitfinexSubscriber(logger *log.Logger) (subber *bitfinexSubscriber) {
	subber = new(bitfinexSubscriber)
	subber.logger = logger
	return
}
