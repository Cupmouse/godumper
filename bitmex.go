package main

import (
	"context"
	"log"
	"net/url"
)

var bitmexChannels = []string{
	"announcement",
	"chat",
	"connected",
	"funding",
	"instrument",
	"insurance",
	"liquidation",
	"orderBookL2",
	"publicNotifications",
	"settlement",
	"trade",
}

type bitmexDump struct {
}

func (d *bitmexDump) Subscribe() ([][]byte, error) {
	return nil, nil
}
func (d *bitmexDump) BeforeConnect() error {
	return nil
}

func dumpBitmex(ctx context.Context, directory string, alwaysDisk bool, logger *log.Logger) error {
	u, serr := url.Parse("wss://www.bitmex.com/realtime")
	if serr != nil {
		return serr
	}
	q := u.Query()
	for _, ch := range bitmexChannels {
		q.Add("subscribe", ch)
	}
	u.RawQuery = q.Encode()
	return dumpNormal(ctx, "bitmex", u.String(), directory, alwaysDisk, logger, &bitmexDump{})
}
