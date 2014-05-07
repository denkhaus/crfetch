package main

import (
	"bitbucket.org/mendsley/tcgl/applog"
	"github.com/denkhaus/go-store"
)

type BarData struct {
	O float64 `json:"o"`
	H float64 `json:"h"`
	L float64 `json:"l"`
	C float64 `json:"c"`
	V float64 `json:"v"`
	T int     `json:"t"`
}

type BarInfo struct {
	Data  BarData `json:"data"`
	TsMin int     `json:"tsMin"`
	TsMax int     `json:"tsMax"`
}

type Normalizer struct {
	store     *store.Store
	snapSteps []int
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (b *BarData) Init(price float64, volume float64, timestamp int) {
	b.O = price
	b.H = price
	b.L = price
	b.C = price
	b.V = volume
	b.T = timestamp
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// normalize
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (n *Normalizer) normalize(ts int, snap int) int {
	return ts - (ts % snap)
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// BuildBar
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (n *Normalizer) BuildPriceBar(prov Provider, quote Quote, snap int) error {

	quoteTs := quote.timeStamp
	price := quote.price
	volume := quote.volume

	barTs := n.normalize(quoteTs, snap)
	barHash := prov.FormatBarHash(quote.symbolId)
	barKey := prov.FormatBarKey(snap, barTs)

	res, err := n.store.HashGet(barHash, barKey)
	if err != nil {
		return err
	}

	if res == nil {
		barData := BarData{}
		barData.Init(price, volume, barTs)
		barInfo := BarInfo{Data: barData, TsMax: quoteTs, TsMin: quoteTs}
		return n.store.HashSet(barHash, barKey, barInfo)
	}

	var barInfo = res.(BarInfo)

	if quoteTs < barInfo.TsMin {
		barInfo.TsMin = quoteTs
		barInfo.Data.O = price
	}

	if quoteTs > barInfo.TsMax {
		barInfo.Data.C = price
		barInfo.Data.V = volume
		barInfo.TsMax = quoteTs
	}

	if price > barInfo.Data.H {
		barInfo.Data.H = price
	}

	if price < barInfo.Data.L {
		barInfo.Data.L = price
	}

	return n.store.HashSet(barHash, barKey, barInfo)
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Do Work
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (n *Normalizer) Normalize(prov Provider) error {
	applog.Infof("start normalizing %s data", prov.Name())

	if err := prov.EnumerateQuotes(func(q Quote) {
		for _, snap := range n.snapSteps {
			n.BuildPriceBar(prov, q, snap)
		}
	}); err != nil {
		return err
	}

	applog.Infof("%s: normalizing successfull, removing quotes", prov.Name())

	if err := prov.RemoveQuotes(); err != nil {
		return err
	}

	return nil
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NewNormalizer
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func NewNormalizer(store *store.Store, snapSteps []int) *Normalizer {
	return &Normalizer{store: store, snapSteps: snapSteps}
}
