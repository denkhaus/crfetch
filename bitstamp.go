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
	etcdClient     *etcd.Client
	bitstampClient *bitstamp.Api
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
		BITSTAMP_PATH_ID, ts, BITSTAMP_MKT_ID_BTCUSD)

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
// create new coinbase provider.
///////////////////////////////////////////////////////////////////////////////////////////////////////
func newBitstampProvider() Provider {
	return &BitstampProvider{}
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// init
///////////////////////////////////////////////////////////////////////////////////////////////////////
func init() {
	RegisterProvider("bitstamp", newBitstampProvider())
}
