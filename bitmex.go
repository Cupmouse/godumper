package main

import (
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

func dumpBitmex(directory string, alwaysDisk bool, logger *log.Logger, stop chan struct{}, errc chan error) {
	defer close(errc)
	var err error
	defer func() {
		if err != nil {
			errc <- err
		}
	}()
	u, serr := url.Parse("wss://www.bitmex.com/realtime")
	if serr != nil {
		err = serr
		return
	}
	q := u.Query()
	for _, ch := range bitmexChannels {
		q.Add("subscribe", ch)
	}
	u.RawQuery = q.Encode()

	err = dumpNormal("bitmex", u.String(), directory, alwaysDisk, logger, &bitmexDump{}, stop)
	return
}
