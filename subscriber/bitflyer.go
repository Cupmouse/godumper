package subscriber

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/exchangedataset/streamcommons/jsonstructs"
)

var bitflyerChannelPrefixes = [...]string{
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

func (d *bitflyerSubscriber) URL() string {
	return "wss://ws.lightstream.bitflyer.com/json-rpc"
}

func (d *bitflyerSubscriber) BeforeConnection() (err error) {
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
	d.productCodes = make([]string, len(markets))
	for i, market := range markets {
		d.productCodes[i] = market.ProductCode
	}
	return
}

func (d *bitflyerSubscriber) Subscribe() (subscribes []Subscribe, err error) {
	subscribes = make([]Subscribe, 0, 100)

	subscribe := new(jsonstructs.BitflyerSubscribe)
	subscribe.Initialize()

	i := 0
	for _, productCode := range d.productCodes {
		for _, prefix := range bitflyerChannelPrefixes {
			subscribe.ID = i
			channel := prefix + productCode
			subscribe.Params.Channel = channel
			var subscribeMessage []byte
			subscribeMessage, err = json.Marshal(subscribe)
			if err != nil {
				return
			}
			subscribes = append(subscribes, Subscribe{Channel: channel, Message: subscribeMessage})
			i++
		}
	}
	return
}

func newBitflyerSubscriber() (subber *bitflyerSubscriber) {
	subber = new(bitflyerSubscriber)
	return
}
