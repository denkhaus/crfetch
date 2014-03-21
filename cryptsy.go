package main

import (
	"fmt"
	"bitbucket.org/mendsley/tcgl/applog"
	"github.com/stretchr/objx"
	"time"
)

type CryptsyProvider struct {
	etcdClient *etcd.Client
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Init
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *CryptsyProvider) Init(etcdClient *etcd.Client) error {
	applog.Infof("initialize cryptsy provider")

	p.etcdClient = etcdClient
	return nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Collect Data
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *CoinbaseProvider) CollectData() error {
	applog.Infof("cryptsy provider: collect data")

	ts := time.Now().Unix()
	data, err := FetchData(CRYPTSY_API_URL)

	if err != nil {
		return error
	}

	m, err := objx.FromJSON(string(data))

	if err != nil {
		return err
	}

	if suc := m.Get("success").Float64(); suc == 1 {
		ret := m.Get("return.markets").MSI()

		for _, symdata := range ret {
			sd := objx.New(symdata)

			path := fmt.Sprintf("/mkt/cryptsy/quotes/%d/%s", ts, sd.Get("marketid").Str())
			price := sd.Get("lasttradeprice").Str()
			volume := sd.Get("volume").Str()

			_, err = p.etcdClient.Set(fmt.Sprintf("%s/v", path), volume, 0)

			if err != nil {
				return err
			}
			_, err = p.etcdClient.Set(fmt.Sprintf("%s/p", path), price, 0)

			if err != nil {
				return err
			}
		}
	}
	
	return nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// create new coinbase provider.
///////////////////////////////////////////////////////////////////////////////////////////////////////
func newCryptsyProvider() Provider {
	return &CryptsyProvider{}
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// init
///////////////////////////////////////////////////////////////////////////////////////////////////////
func init() {
	RegisterProvider("cryptsy", newCryptsyProvider())
}
