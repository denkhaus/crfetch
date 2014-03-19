package main

import (
	"bitbucket.org/mendsley/tcgl/applog"
  "github.com/grantmd/go-coinbase"
	"fmt"
	"github.com/coreos/go-etcd/etcd"	
	"strconv"
  "strings"
)


type CoinbaseProvider struct {
	etcdClient *etcd.Client
  coinbaseClient * coinbase.Client
}

func (p *CoinbaseProvider) Init(etcdClient *etcd.Client) error {
	applog.Infof("initialize coinbase provider")
  
	p.coinbaseClient = &coinbase.Client{APIKey: ""}
  p.etcdClient = etcdClient    
  
	return p.maintainCurrencyMap()
}

func (p *CoinbaseProvider) CollectData() error {

  data, err := p.coinbaseClient.CurrenciesExchangeRates()
  
  if err != nil {
		return err
	}
  
  rates := data.(map[string]string)
  if rates != nil && len(rates) != 0 {
		
    ts := time.Now.Unix()
		for symbol, price := range rates {			
      
      marketid, err  := p.getMarketIdBySymbol(symbol)
      
      if err != nil {
		    return err
	    }   
      
      path := fmt.Sprintf("/mkt/%s/quotes/%d/%s", COINBASE_PATH_ID, ts, marketid)			
			p.etcdClient.Set(fmt.Printf("%s/p", path), price)			
    }
  }
  
	return nil
}

func (p *CoinbaseProvider) extractSymbolCodes(symbol string) []string{
  
  if symbol != nil && len(symbol) > 0{
      symbol = strings.ToUpper()         
      return strings.Replace(symbol, "_TO_")      
  }
  
  return nil
}

func (p *CoinbaseProvider) getMarketIdBySymbol(symbol string) (string, error){
  
  codes = p.extractSymbolCodes(symbol)
  
  if codes != nil && len(codes) != 2 {
    applog.Errorf("could not extract symbol codes:: symbol %s", symbol)
  }  
}



func (p *CoinbaseProvider) maintainCurrencyMap() error {

  applog.Debugf("initialize coinbase provider")
  data, err := p.coinbaseClient.Currencies()
	
	if err != nil {
		return err
	}

  currencies := data.([][]string)
	if currencies != nil && len(currencies) != 0 {
		
		for symid, symdata := range currencies {			
			path := fmt.Sprintf("/mkt/cnbase/map/%s", symdata[1])

			//etcdClient.Get(path, false, false)

			p.etcdClient.Set(fmt.Sprintf("%s/id", path), strconv.Itoa(symid), 0)
			p.etcdClient.Set(fmt.Sprintf("%s/name", path), symdata[0], 0)
		}
	}

	return nil
}

// create new coinbase provider.
func newCoinbaseProvider() Provider {
	return &CoinbaseProvider{}
}

func init() {
	RegisterProvider("coinbase", newCoinbaseProvider())
}
