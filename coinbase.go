package main

import (
	"bitbucket.org/mendsley/tcgl/applog"
	"fmt"
	"github.com/coreos/go-etcd/etcd"
	"github.com/stretchr/objx"
	"strconv"
)

type CoinbaseProvider struct {
	etcdClient *etcd.Client
}

func (p *CoinbaseProvider) Init(etcdClient *etcd.Client) error {
	applog.Infof("initialize coinbase provider")

	p.etcdClient = etcdClient
	return p.maintainCurrencyMap()
}

func (p *CoinbaseProvider) CollectData() error {

	return nil
}

func (p *CoinbaseProvider) maintainCurrencyMap() error {

	data, err := FetchData(COINBASE_API_CURRENCIES_URL)

	if err != nil {
		return err
	}

	if data != nil && len(data) != 0 {

		jdata := fmt.Sprintf("{ \"data\": %s}", string(data))
		m, err := objx.FromJSON(jdata)

		if err != nil {
			return err
		}

		k := m.Get("data").Data()
		for symid, symdata := range k.([]interface{}) {
			f := symdata.([]interface{})
			path := fmt.Sprintf("/mkt/cnbase/map/%s", f[1])

			//etcdClient.Get(path, false, false)

			p.etcdClient.Set(fmt.Sprintf("%s/id", path), strconv.Itoa(symid), 0)
			p.etcdClient.Set(fmt.Sprintf("%s/name", path), f[0].(string), 0)
		}
	}

	return nil
}

// create new coinbase provider.
func NewCoinbaseProvider() *CoinbaseProvider {
	return &CoinbaseProvider{}
}

func init() {
	RegisterProvider("coinbase", NewCoinbaseProvider())
}
