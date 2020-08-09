package main

type bitmexSubscriber struct{}

func (s *bitmexSubscriber) URL() string {
	return "wss://www.bitmex.com/realtime?subscribe=announcement,chat,connected,funding,instrument,insurance,liquidation,orderBookL2,publicNotifications,settlement,trade"
}

func (s *bitmexSubscriber) BeforeConnection() error {
	// nothing to prepare
	return nil
}

func (s *bitmexSubscriber) AfterSubscribed() ([]queueElement, error) {
	return nil, nil
}

func (s *bitmexSubscriber) Subscribe() ([][]byte, error) {
	// nothing to return
	return nil, nil
}

func newBitmexSubscriber() (subber *bitmexSubscriber) {
	subber = new(bitmexSubscriber)
	return
}
