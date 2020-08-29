package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/exchangedataset/streamcommons/jsonstructs"
)

var bitbankPrefixes = [...]string{
	"ticker_",
	"transactions",
	"depth_diff_",
	"depth_whole_",
}

var bitbankParis = [...]string{
	"btc_jpy",
	"xrp_jpy",
	"xrp_btc",
	"ltc_jpy",
	"ltc_btc",
	"eth_jpy",
	"eth_btc",
	"mona_jpy",
	"mona_btc",
	"bcc_jpy",
	"bcc_btc",
}

type bitbankDump struct {
}

func (d *bitbankDump) Subscribe() ([][]byte, error) {
	subs := make([][]byte, len(bitbankPrefixes)*len(bitbankParis))
	for i, pair := range bitbankParis {
		for j, prefix := range bitbankPrefixes {
			s := new(jsonstructs.BitbankSubscribe)
			s.Initialize()
			s[1] = prefix + pair
			m, serr := json.Marshal(s)
			if serr != nil {
				return nil, serr
			}
			sub := make([]byte, len(m)+2)
			sub[0], sub[1] = '4', '2'
			copy(sub[2:], m)
			subs[i*len(bitbankPrefixes)+j] = sub
		}

	}
	return subs, nil
}

func (d *bitbankDump) BeforeConnect(ctx context.Context) error {
	return nil
}

func dumpBitbank(ctx context.Context, directory string, alwaysDisk bool, logger *log.Logger) error {
	return dumpNormal(ctx, "bitbank", "wss://stream.bitbank.cc/socket.io/?EIO=3&transport=websocket", directory, alwaysDisk, logger, &bitbankDump{})
}
