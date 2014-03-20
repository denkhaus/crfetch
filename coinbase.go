package main

import (
	"bitbucket.org/mendsley/tcgl/applog"
  "github.com/grantmd/go-coinbase"
	"fmt"
	"github.com/denkhaus/go-etcd/etcd"	
  "time"
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
  
	return p.maintainCurrencyNamesMap()
}

func (p *CoinbaseProvider) CollectData() error {
  applog.Infof("coinbase provider: collect data")
  
  data, err := p.coinbaseClient.CurrenciesExchangeRates()
  
  if err != nil {
		return err
	}
  
  rates := coinbase.Rates(data)
  if rates != nil && len(rates) != 0 {
		
    ts := time.Now().Unix()
		for symbol, price := range rates {			
      
      marketid, err  := p.getMarketIdBySymbol(symbol)
      
      if err != nil {
		    return err
	    }   
      
      path := fmt.Sprintf("/mkt/%s/quotes/%d/%s/p", COINBASE_PATH_ID, ts, marketid)			
      _, err := p.etcdClient.Set(path, price, 0)		
      
      if err != nil {
		    return err
	    }   
      
    }
  }
  
	return nil
}

func (p *CoinbaseProvider) getValidSymbolCode(symbol string) string{
  
  if len(symbol) > 0{
      symbol = strings.ToUpper(symbol)         
      return strings.Replace(symbol, "_TO_","-", -1)      
  }
  
  return ""
}

func (p *CoinbaseProvider) getMarketIdBySymbol(symbol string) (string, error){
  
  code := p.getValidSymbolCode(symbol)
  
  if len(code) == 0 {
    return "", fmt.Errorf("could not extract symbol code:: symbol %s", symbol)
  }  
  
  basePath := fmt.Sprintf("/mkt/%s/map", COINBASE_PATH_ID)
  keyPath := fmt.Sprintf("%s/%s/id",basePath, code)
  value, err := p.etcdClient.GetValue(keyPath)
  
  if err != nil {
     return "", err 
  }
  
  if len(value) > 0 {
     return value, nil 
  }
  
  count, err := p.etcdClient.KeyCount(basePath)
  
  if err != nil {
     return "", err 
  }
  
  value = strconv.Itoa(count)
  _, err = p.etcdClient.Set(keyPath, value, 0)
  
  if err != nil {
		return "", err
	}
  
  return value, nil  
}

func (p *CoinbaseProvider) maintainCurrencyNamesMap() error {

  applog.Infof("coinbase: maintain currency names map")
  data, err := p.coinbaseClient.Currencies()
	
	if err != nil {
		return err
	}

  currencies := data.([][]string)
	if currencies != nil && len(currencies) != 0 {
		
		for _, symdata := range currencies {			
			path := fmt.Sprintf("/mkt/%s/map/symnames/%s", COINBASE_PATH_ID, symdata[1])						
      _, err := p.etcdClient.Set(fmt.Sprintf("%s/name", path), symdata[0], 0)
      
      if err != nil{
        return err  
      }
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
