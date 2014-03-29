package main

import (
	"bitbucket.org/mendsley/tcgl/applog"
	"fmt"
	"github.com/Narsil/bitstamp-go"
	"github.com/denkhaus/go-etcd/etcd"
	"strconv"
	"time"
)

type BitstampProvider struct {
	ProviderBase
	bitstampClient *bitstamp.Api
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// FormatPriceKey
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *BitstampProvider) FormatPriceKey(ts int, symbolId int) string {
	return fmt.Sprintf("%s/%d/bid", p.FormatTimestampPath(ts), symbolId)
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Init
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *BitstampProvider) Init(etcdClient *etcd.Client) (err error) {
	applog.Infof("initialize bitstamp provider")

	p.bitstampClient, err = bitstamp.New("", "")
	p.etcdClient = etcdClient
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

	path := fmt.Sprintf("/mkt/%s/quotes/%d/%s",
		p.pathId, ts, BITSTAMP_MKT_ID_BTCUSD)

	value := strconv.FormatFloat(data.Bid, 'g', 1, 64)
	_, err = p.etcdClient.Set(fmt.Sprintf("%s/bid", path), value, 0)

	if err != nil {
		return
	}

	value = strconv.FormatFloat(data.Ask, 'g', 1, 64)
	_, err = p.etcdClient.Set(fmt.Sprintf("%s/ask", path), value, 0)

	if err != nil {
		return
	}

	value = strconv.FormatFloat(data.Last, 'g', 1, 64)
	_, err = p.etcdClient.Set(fmt.Sprintf("%s/last", path), value, 0)

	if err != nil {
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
	provider.pathId = BITSTAMP_PATH_ID
	provider.self = provider
	RegisterProvider(provider.Name(), provider)
}
