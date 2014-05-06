package main

import (
	"bitbucket.org/mendsley/tcgl/applog"
	"fmt"
	"github.com/denkhaus/go-store"
	"path"
	"strconv"
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
// RemoveQuoteData
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (n *Normalizer) RemoveQuoteData(prov Provider, symbolId, ts int) error {

	score := float64(ts)
	priceSetName := prov.FormatPriceKey(symbolId)
	if _, err := n.store.SortedSetDeleteByScore(priceSetName, score, score); err != nil {
		return fmt.Errorf("unable to remove price info from %s, ts %d :: error:: %s",
			priceSetName, ts, err.Error())
	}

	volumeSetName := prov.FormatVolumeKey(symbolId)
	if len(volumeSetName) > 0 {
		if _, err := n.store.SortedSetDeleteByScore(volumeSetName, score, score); err != nil {
			return fmt.Errorf("unable to remove volume info from %s, ts %d :: error:: %s",
				volumeSetName, ts, err.Error())
		}
	}

	return nil
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// BuildBar
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (n *Normalizer) BuildPriceBar(prov Provider, price float64, volume float64, symbolId int, snap int, quoteTs int) error {

	barTs := n.normalize(quoteTs, snap)
	barHash := prov.FormatBarHash(symbolId)
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
// NormalizeSymbols
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (n *Normalizer) NormalizeSymbols(prov Provider, snap int) error {

	price, err := prov.GetPrice(ts, symbolId)
	if err != nil {
		return err
	}

	volume, err := prov.GetVolume(ts, symbolId)
	if err != nil {
		return err
	}

	return n.BuildPriceBar(prov, price, volume, symbolId, snap, ts)
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Do Work
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (n *Normalizer) Normalize(prov Provider) error {
	applog.Infof("start normalizing %s data", prov.Name())

	for _, snap := range n.snapSteps {
		if err = n.NormalizeSymbols(prov, snap); err != nil {
			return err
		}
	}

	applog.Infof("%s: normalizing successfull, removing ts %d quotes", prov.Name(), ts)

	err = n.RemoveQuoteData(prov)
	if err != nil {
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
