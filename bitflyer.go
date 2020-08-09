package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/exchangedataset/streamcommons/jsonstructs"
)

var bitflyerChannelPrefixes = []string{
	"lightning_executions_",
	"lightning_board_snapshot_",
	"lightning_board_",
	"lightning_ticker_",
}

type bitflyerSubscriber struct {
	productCodes []string
}

type bitflyerMarketsElement struct {
	ProductCode string `json:"product_code"`
}

func (s *bitflyerSubscriber) URL() string {
	return "wss://ws.lightstream.bitflyer.com/json-rpc"
}

func (s *bitflyerSubscriber) BeforeConnection() (err error) {
	// fetch what markets they have
	var res *http.Response
	res, err = http.Get("https://api.bitflyer.com/v1/markets")
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
	if err != nil {
		return
	}

	markets := make([]bitflyerMarketsElement, 0, 100)
	err = json.Unmarshal(resBytes, &markets)
	if err != nil {
		return
	}

	// response have the format of [{'product_code':'BTC_JPY'},{...}...]
	// produce an array of product_code
	s.productCodes = make([]string, len(markets))
	for i, market := range markets {
		s.productCodes[i] = market.ProductCode
	}
	return
}

func (s *bitflyerSubscriber) AfterSubscribed() ([]queueElement, error) {
	return nil, nil
}

func (s *bitflyerSubscriber) Subscribe() ([][]byte, error) {
	subscribes := make([][]byte, 0, 100)

	subscribe := new(jsonstructs.BitflyerSubscribe)
	subscribe.Initialize()

	i := 0
	for _, productCode := range s.productCodes {
		for _, prefix := range bitflyerChannelPrefixes {
			subscribe.ID = i
			channel := prefix + productCode
			subscribe.Params.Channel = channel
			subscribeMessage, serr := json.Marshal(subscribe)
			if serr != nil {
				return nil, serr
			}
			subscribes = append(subscribes, subscribeMessage)
			i++
		}
	}
	return subscribes, nil
}

func newBitflyerSubscriber() (subber *bitflyerSubscriber) {
	subber = new(bitflyerSubscriber)
	return
}
