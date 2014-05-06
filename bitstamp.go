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
// FormatPriceKey
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *BitstampProvider) FormatPriceKey(symbolId int) string {
	return fmt.Sprintf("%s/bid", p.FormatSymbolIdPath(symbolId))
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
	data, err := p.bitstampClient.GetTicker()
	if err != nil {
		return
	}

	setName := fmt.Sprintf("/mkt/%s/q/%s", p.pathId, BITSTAMP_MKT_ID_BTCUSD)

	if _, err = p.store.SortedSetSet(fmt.Sprintf("%s/bid", setName),
		float64(ts), data.Bid); err != nil {
		return
	}

	if _, err = p.store.SortedSetSet(fmt.Sprintf("%s/ask", setName),
		float64(ts), data.Ask); err != nil {
		return
	}

	if _, err = p.store.SortedSetSet(fmt.Sprintf("%s/last", setName),
		float64(ts), data.Last); err != nil {
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
