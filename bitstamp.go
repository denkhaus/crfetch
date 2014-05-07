package main

import (
	"bitbucket.org/mendsley/tcgl/applog"
	"fmt"
	"github.com/Narsil/bitstamp-go"
	"github.com/denkhaus/go-store"
	"github.com/denkhaus/yamlconfig"
	"time"
)

type BitstampProvider struct {
	ProviderBase
	bitstampClient *bitstamp.Api
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Init
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *BitstampProvider) Init(config *yamlconfig.Config, store *store.Store) (err error) {
	applog.Infof("initialize bitstamp provider")
	p.bitstampClient, err = bitstamp.New("", "")
	p.config = config
	p.store = store
	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Collect Data
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *BitstampProvider) CollectData() (err error) {
	applog.Infof("bitstamp provider: collect data")

	ts := time.Now().Unix()
	ticker, err := p.bitstampClient.GetTicker()
	if err != nil {
		return
	}

	setName := fmt.Sprintf("/mkt/%s/q/%s", p.pathId, BITSTAMP_MKT_ID_BTCUSD)
	data := map[string]interface{}{"bid": ticker.Bid, "ask": ticker.Ask, "last": ticker.Last, "t": ts}
	if _, err = p.store.SortedSetSet(setName, float64(ts), data); err != nil {
		return
	}

	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// init
///////////////////////////////////////////////////////////////////////////////////////////////////////
func init() {
	provider := &BitstampProvider{}
	provider.name = "bitstamp"
	provider.pathId = "btstmp"
	provider.self = provider
	RegisterProvider(provider.Name(), provider)
}
