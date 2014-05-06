package main

import (
	"bitbucket.org/mendsley/tcgl/applog"
	"fmt"
	"github.com/denkhaus/go-coinbase"
	"github.com/denkhaus/go-store"
	"github.com/denkhaus/yamlconfig"
	"strconv"
	"strings"
	"time"
)

type CoinbaseProvider struct {
	ProviderBase
	coinbaseClient *coinbase.Client
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Init
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *CoinbaseProvider) Init(config *yamlconfig.Config, store *store.Store) error {
	applog.Infof("initialize coinbase provider")

	p.coinbaseClient = &coinbase.Client{
		APIKey:	config.GetString("provider:coinbase:apikey"),
	}

	p.config = config
	p.store = store

	return p.maintainCurrencyNamesMap()
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Collect Data
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *CoinbaseProvider) CollectData() error {
	applog.Infof("coinbase provider: collect data")

	rates, err := p.coinbaseClient.GetExchangeRates()
	if err != nil {
		return err
	}

	if rates != nil && len(rates) != 0 {
		ts := time.Now().Unix()
		for symbol, price := range rates {
			marketid, err := p.getMarketIdBySymbol(symbol)
			if err != nil {
				return err
			}

			pr, err := strconv.ParseFloat(price, 64)
			if err != nil {
				return err
			}

			setName := fmt.Sprintf("/mkt/%s/q/%s/p", p.pathId, marketid)
			if _, err = p.store.SortedSetSet(setName, float64(ts), pr); err != nil {
				return err
			}
		}
	}

	return nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
//
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *CoinbaseProvider) getValidSymbolCode(symbol string) string {

	if len(symbol) > 0 {
		symbol = strings.ToUpper(symbol)
		return strings.Replace(symbol, "_TO_", "-", -1)
	}

	return ""
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
//
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *CoinbaseProvider) getMarketIdBySymbol(symbol string) (string, error) {

	code := p.getValidSymbolCode(symbol)

	if len(code) == 0 {
		return "", fmt.Errorf("could not extract symbol code:: symbol %s", symbol)
	}

	hash := fmt.Sprintf("/mkt/%s/map/pairs/", p.pathId)
	key := fmt.Sprintf("%s/id", code)

	value, err := p.store.HashGet(hash, key)
	if err != nil {
		return "", err
	}

	if value != nil {
		return value.(string), nil
	}

	count, err := p.store.HashSize(hash)
	if err != nil {
		return "", err
	}

	value = strconv.Itoa(count)
	err = p.store.HashSet(hash, key, value)
	if err != nil {
		return "", err
	}

	return value.(string), nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Maintains Currency Names Map to ISO Symbol
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *CoinbaseProvider) maintainCurrencyNamesMap() error {
	applog.Infof("coinbase: maintain currency names map")

	curr, err := p.coinbaseClient.GetCurrencies()
	if err != nil {
		return fmt.Errorf("unable to get currencies error:: %s",
			err.Error())
	}

	if curr != nil && len(curr) != 0 {
		hash := fmt.Sprintf("/mkt/%s/map/symbols", p.pathId)
		for _, symdata := range curr {
			if err = p.store.HashSet(hash, symdata[1], symdata[0]); err != nil {
				return fmt.Errorf("maintain currency names map error:: %s", err.Error())
			}
		}
	}

	return nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// init
///////////////////////////////////////////////////////////////////////////////////////////////////////
func init() {
	provider := &CoinbaseProvider{}
	provider.name = "coinbase"
	provider.pathId = "cnbase"
	provider.self = provider
	RegisterProvider(provider.Name(), provider)
}
