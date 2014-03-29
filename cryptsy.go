package main

import (
	"bitbucket.org/mendsley/tcgl/applog"
	"fmt"
	"github.com/denkhaus/go-etcd/etcd"
	"github.com/stretchr/objx"
	"time"
)

type CryptsyProvider struct {
	ProviderBase
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// FormatPriceKey
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *CryptsyProvider) FormatVolumeKey(ts int, symbolId int) string {
	return fmt.Sprintf("%s/%d/v", p.FormatTimestampPath(ts), symbolId)
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
func (p *CryptsyProvider) CollectData() (err error) {
	applog.Infof("cryptsy provider: collect data")

	ts := time.Now().Unix()
	data, err := FetchData(CRYPTSY_API_URL)

	if err != nil {
		return
	}

	m, err := objx.FromJSON(string(data))

	if err != nil {
		return
	}

	if suc := m.Get("success").Float64(); suc == 1 {
		ret := m.Get("return.markets").MSI()

		for _, symdata := range ret {
			sd := objx.New(symdata)

			path := fmt.Sprintf("/mkt/%s/quotes/%d/%s",
				CRYPTSY_PATH_ID, ts, sd.Get("marketid").Str())

			price := sd.Get("lasttradeprice").Str()
			volume := sd.Get("volume").Str()

			_, err = p.etcdClient.Set(fmt.Sprintf("%s/v", path), volume, 0)

			if err != nil {
				return
			}

			_, err = p.etcdClient.Set(fmt.Sprintf("%s/p", path), price, 0)

			if err != nil {
				return
			}
		}
	}

	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// init
///////////////////////////////////////////////////////////////////////////////////////////////////////
func init() {
	provider := &CryptsyProvider{}
	provider.name = "cryptsy"
	provider.pathId = CRYPTSY_PATH_ID
	provider.self = provider
	RegisterProvider(provider.Name(), provider)
}
