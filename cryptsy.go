package main

import (
	"bitbucket.org/mendsley/tcgl/applog"
	"fmt"
	"github.com/denkhaus/go-store"
	"github.com/denkhaus/yamlconfig"
	"github.com/stretchr/objx"
	"strconv"
	"time"
)

type CryptsyProvider struct {
	ProviderBase
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// FormatPriceKey
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *CryptsyProvider) FormatVolumeKey(symbolId int) string {
	return fmt.Sprintf("%s/v", p.FormatSymbolIdPath(symbolId))
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Init
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *CryptsyProvider) Init(config *yamlconfig.Config, store *store.Store) error {
	applog.Infof("initialize cryptsy provider")
	p.config = config
	p.store = store
	return nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Collect Data
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *CryptsyProvider) CollectData() error {
	applog.Infof("cryptsy provider: collect data")

	ts := time.Now().Unix()
	data, err := FetchData(CRYPTSY_API_URL)
	if err != nil {
		return err
	}

	m, err := objx.FromJSON(string(data))
	if err != nil {
		return err
	}

	if suc := m.Get("success").Float64(); suc == 1 {
		ret := m.Get("return.markets").MSI()

		for _, symdata := range ret {
			sd := objx.New(symdata)

			setName := fmt.Sprintf("/mkt/%s/q/%s",p.pathId, sd.Get("marketid").Str())
	        
            vol , err := strconv.ParseFloat(sd.Get("volume").Str(),64)
            if err != nil {
		        return err
	        }
            
			if _, err = p.store.SortedSetSet(
			    fmt.Sprintf("%s/v", setName), float64(ts), vol); err != nil {
				return err
			}

            pr , err := strconv.ParseFloat(sd.Get("lasttradeprice").Str(),64)
            if err != nil {
		        return err
	        }
	        
			if _, err = p.store.SortedSetSet(
			    fmt.Sprintf("%s/p", setName), float64(ts), pr); err != nil {
				return err
			}
		}
	}

	return err
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// init
///////////////////////////////////////////////////////////////////////////////////////////////////////
func init() {
	provider := &CryptsyProvider{}
	provider.name = "cryptsy"
	provider.pathId = "crtsy"
	provider.self = provider
	RegisterProvider(provider.Name(), provider)
}
