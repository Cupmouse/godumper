package subscriber

type bitmexSubscriber struct {
}

func (d *bitmexSubscriber) URL() string {
	return "wss://www.bitmex.com/realtime?subscribe=announcement,chat,connected,funding,instrument,insurance,liquidation,orderBookL2,publicNotifications,settlement,trade"
}

func (d *bitmexSubscriber) BeforeConnection() error {
	// nothing to prepare
	return nil
}

func (d *bitmexSubscriber) Subscribe() (subscribes []Subscribe, err error) {
	// nothing to return
	subscribes = make([]Subscribe, 0)
	return
}

func newBitmexSubscriber() (subber *bitmexSubscriber) {
	subber = new(bitmexSubscriber)
	return
}
