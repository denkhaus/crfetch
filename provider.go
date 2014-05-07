package main

import (
	"fmt"
	"github.com/denkhaus/go-store"
	"github.com/denkhaus/yamlconfig"
)

type Quote struct {
	price     float64
	volume    float64
	symbolId  int
	timeStamp int
}

type ProviderBase struct {
	store  *store.Store
	config *yamlconfig.Config
	self   Provider
	name   string
	pathId string
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Name
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *ProviderBase) Name() string {
	return p.name
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// FormatBarHash
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *ProviderBase) FormatBarHash(symbolId int) string {
	return fmt.Sprintf("/mkt/%s/bars/%d", p.pathId, symbolId)
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// FormatBarKey
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *ProviderBase) FormatBarKey(snap int, barTs int) string {
	return fmt.Sprintf("%s/%d", snap, barTs)
}

type EnumQuotesFunc func(quote Quote)

///////////////////////////////////////////////////////////////////////////////////////////////////////
// EnumQuotesFunc
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *ProviderBase) EnumerateQuotes(enumQuotesFunc EnumQuotesFunc) error {

	var (
		pathId   string
		symbolId int
	)

	match := fmt.Sprintf("/mkt/%s/q", p.pathId)
	p.store.EnumerateKeys(match, func(idx int, key string) error {

		c, err := fmt.Sscanf(key, "/mkt/%s/q/%d", &pathId, &symbolId)
		if err != nil || c != 2 {
			return fmt.Errorf("error while parsing key %s:: %s", key, err.Error())
		}

		res, err := p.store.SortedSetGetAll(key)
		if err != nil{
			return err
		}
		
		if res != nil && len(res) > 0{
			for _, data = range res{
				quote =: Quote{symbolId: symbolId}				
				quote.volume = data["vol"]
				
				if quote.timeStamp, ok := data["ts"]; !ok{
					return fmt.Errorf("parsing quotedata of key %s. No timestamp info available.", key)
				}
				
				if quote.price, ok := data["bid"]; !ok{
					return fmt.Errorf("parsing quotedata of key %s. No price info available.", key)
				}				
			}
		}
		
		return nil
	})

	return nil
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RemoveQuotes
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *ProviderBase) RemoveQuotes() error {

	match := fmt.Sprintf("/mkt/%s/q", p.pathId)
	p.store.EnumerateKeys(match, func(idx int, key string) error {
		if _, err := p.store.SortedSetDeleteAll(key); err != nil {
			return fmt.Errorf("unable to remove price info from %s:: error:: %s",
				key, err.Error())
		}
		return nil
	})

	return nil
}
