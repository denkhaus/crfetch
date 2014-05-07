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

///////////////////////////////////////////////////////////////////////////////////////////////////////
// FormatQuotesPath
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (p *ProviderBase) FormatQuotesPath() string {
	return fmt.Sprintf("/mkt/%s/q", p.pathId)
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

	p.store.EnumerateKeys(p.FormatQuotesPath(), func(idx int, key string) error {

		if c, err := fmt.Sscanf(key, "/mkt/%s/q/%d", &pathId, &symbolId); err != nil || c != 2 {
			return fmt.Errorf("error while parsing key %s:: %s", key, err.Error())
		}

		res, err := p.store.SortedSetGetAll(key)
		if err != nil {
			return err
		}

		if res != nil && len(res) > 0 {
			for _, d := range res {
				data := d.(QuoteStoreData)

				quote := Quote{symbolId: symbolId}
				quote.volume = data["vol"].(float64)

				if ts, ok := data["ts"]; ok {
					quote.timeStamp = ts.(int)
				} else {
					return fmt.Errorf("parsing quotedata of key %s. No timestamp info available.", key)
				}

				if price, ok := data["bid"]; ok {
					quote.price = price.(float64)
				} else {
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

	p.store.EnumerateKeys(p.FormatQuotesPath(), func(idx int, key string) error {
		if _, err := p.store.SortedSetDeleteAll(key); err != nil {
			return fmt.Errorf("unable to remove price info from %s:: error:: %s",
				key, err.Error())
		}
		return nil
	})

	return nil
}
