package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"

	"github.com/exchangedataset/streamcommons/jsonstructs"
)

const liquidChannelLimit = 10
const liquidUSDJPY = 100

type liquidDump struct {
	logger *log.Logger
	// This is in lower-case
	largeVolumeProds []string
}

func (d *liquidDump) generateSubscribe(channel string) ([]byte, error) {
	data := new(jsonstructs.LiquidSubscribeData)
	data.Channel = channel
	md, serr := json.Marshal(data)
	if serr != nil {
		return nil, fmt.Errorf("data marshal: %v", serr)
	}
	s := new(jsonstructs.LiquidMessageRoot)
	s.Event = jsonstructs.LiquidEventSubscribe
	s.Data = md
	ms, serr := json.Marshal(s)
	if serr != nil {
		return nil, fmt.Errorf("subcribe marshal: %v", serr)
	}
	return ms, nil
}

func (d *liquidDump) Subscribe() ([][]byte, error) {
	subs := make([][]byte, 3*len(d.largeVolumeProds))
	for i, pair := range d.largeVolumeProds {
		s, serr := d.generateSubscribe("price_ladders_cash_" + pair + "_buy")
		if serr != nil {
			return nil, serr
		}
		subs[3*i+0] = s
		s, serr = d.generateSubscribe("price_ladders_cash_" + pair + "_sell")
		if serr != nil {
			return nil, serr
		}
		subs[3*i+1] = s
		s, serr = d.generateSubscribe("executions_cash_" + pair)
		if serr != nil {
			return nil, serr
		}
		subs[3*i+2] = s
	}
	return subs, nil
}

type liquidProduct struct {
	pairCode  string
	usdVolume float64
}

type liquidProducts []liquidProduct

func (a liquidProducts) Len() int           { return len(a) }
func (a liquidProducts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a liquidProducts) Less(i, j int) bool { return a[i].usdVolume < a[j].usdVolume }

func (d *liquidDump) BeforeConnect(ctx context.Context) error {
	productsBody, _, serr := rest(ctx, "https://api.liquid.com/products")
	if serr != nil {
		return fmt.Errorf("product rest: %v", serr)
	}
	products := make([]jsonstructs.LiquidProduct, 0, 400)
	serr = json.Unmarshal(productsBody, &products)
	if serr != nil {
		return fmt.Errorf("products unmarshal: %v", serr)
	}
	priceTable := make(map[string]float64)
	for _, prod := range products {
		var lastPrice24h float64
		if prod.LastPrice24h != nil {
			var serr error
			lastPrice24h, serr = strconv.ParseFloat(*prod.LastPrice24h, 64)
			if serr != nil {
				return fmt.Errorf("average price: %v", serr)
			}
		}
		var lastTradedPrice float64
		if prod.LastTradedPrice != nil {
			var serr error
			lastTradedPrice, serr = strconv.ParseFloat(*prod.LastTradedPrice, 64)
			if serr != nil {
				return fmt.Errorf("last traded price: %v", serr)
			}
		}
		price := (lastPrice24h + lastTradedPrice) / 2
		if price == 0 {
			d.logger.Println("price is zero:", prod.CurrencyPairCode)
		}
		priceTable[prod.CurrencyPairCode] = price
	}
	productUSDVolumes := make(liquidProducts, len(products))
	for i, prod := range products {
		var vol float64
		if prod.Volume24h != nil {
			var serr error
			vol, serr = strconv.ParseFloat(*prod.Volume24h, 64)
			if serr != nil {
				return fmt.Errorf("volume24h: %v", serr)
			}
		}
		var usdVolume float64
		if prod.QuotedCurrency == "USDT" || prod.QuotedCurrency == "USD" {
			usdVolume = vol * priceTable[prod.CurrencyPairCode]
		} else if prod.QuotedCurrency == "JPY" {
			d.logger.Println("using uncertain USDJPY price for ", prod.CurrencyPairCode)
			usdVolume = vol * priceTable[prod.CurrencyPairCode] / liquidUSDJPY
		} else {
			usdPrice, ok := priceTable[prod.BaseCurrency+"USD"]
			if !ok {
				usdPrice, ok = priceTable[prod.BaseCurrency+"USDT"]
			}
			if !ok {
				quoteUSDPrice, ok := priceTable[prod.QuotedCurrency+"USD"]
				if !ok {
					quoteUSDPrice, ok = priceTable[prod.QuotedCurrency+"USDT"]
				}
				if !ok {
					d.logger.Println("could not find usd market for", prod.CurrencyPairCode)
				}
				usdPrice = priceTable[prod.CurrencyPairCode] * quoteUSDPrice
			}
			usdVolume = usdPrice * vol
		}
		if usdVolume == 0 {
			d.logger.Println("usd volume is zero:", prod.CurrencyPairCode)
		}
		productUSDVolumes[i] = liquidProduct{
			pairCode:  prod.CurrencyPairCode,
			usdVolume: usdVolume,
		}
	}
	sort.Sort(sort.Reverse(productUSDVolumes))
	largeVolumeProds := make([]string, liquidChannelLimit)
	for i := 0; i < len(productUSDVolumes) && i < liquidChannelLimit; i++ {
		largeVolumeProds[i] = strings.ToLower(productUSDVolumes[i].pairCode)
	}
	d.largeVolumeProds = largeVolumeProds
	d.logger.Println("choosen markets:", largeVolumeProds)
	return nil
}

func dumpLiquid(ctx context.Context, directory string, alwaysDisk bool, logger *log.Logger) error {
	return dumpNormal(ctx, "liquid", "wss://tap.liquid.com/app/LiquidTapClient", directory, alwaysDisk, logger, &liquidDump{logger: logger})
}
