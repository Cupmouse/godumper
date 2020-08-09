package main

import (
	"fmt"
	"log"
)

// Subscriber does subscription for dumper
type Subscriber interface {
	// return URL dumper is supposed to connect to
	URL() string
	// BeforeConnection is called before establishing connection to the server. Used to collect market information
	// so it could pick what channel to subscribe.
	BeforeConnection() error
	// Subscribe returns subscribe request message
	Subscribe() ([][]byte, error)
	// Called after subscribe requests are sent
	AfterSubscribed() ([]queueElement, error)
}

// GetSubscriber returns the right subscriber for specified exchange
func GetSubscriber(exchange string, logger *log.Logger) (subscriber Subscriber, err error) {
	switch exchange {
	case "bitfinex":
		subscriber = newBitfinexSubscriber(logger)
		break
	case "bitmex":
		subscriber = newBitmexSubscriber()
		break
	case "bitflyer":
		subscriber = newBitflyerSubscriber()
		break
	case "binance":
		subscriber = newBinanceSubscriber(logger)
		break
	default:
		err = fmt.Errorf("dumper for exchange %s is not supported", exchange)
		break
	}

	return
}
