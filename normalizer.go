package main

import (
	"bitbucket.org/mendsley/tcgl/applog"
	"encoding/json"
	"fmt"
	"github.com/denkhaus/go-etcd/etcd"
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
	etcdClient *etcd.Client
	snapSteps  []int
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
func (n *Normalizer) RemoveQuoteData(prov Provider, ts int) error {

	tsPath := prov.FormatTimestampPath(ts)
	_, err := n.etcdClient.Delete(tsPath, true)

	if err != nil {
		return fmt.Errorf("unable to remove dataPath %s :: error:: %s",
			tsPath, err.Error())
	}

	return nil
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PersistBarInfo
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (n *Normalizer) PersistBarInfo(key string, info *BarInfo) error {

	bi, err := json.Marshal(*info)

	if err != nil {
		return fmt.Errorf("unable to marshal new BarInfo:: error:: %s",
			err.Error())
	}

	barInfoString := string(bi)
	_, err = n.etcdClient.Set(key, barInfoString, 0)

	if err != nil {
		return fmt.Errorf("unable to persist BarInfo: %s:: error:: %s",
			barInfoString, err.Error())
	}

	return nil
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// CreateNewBar
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (n *Normalizer) CreateNewBar(key string, price float64, volume float64, quoteTs int, barTs int) error {

	barData := BarData{}
	barData.Init(price, volume, barTs)
	barInfo := BarInfo{Data: barData, TsMax: quoteTs, TsMin: quoteTs}

	return n.PersistBarInfo(key, &barInfo)
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// BuildBar
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (n *Normalizer) BuildPriceBar(prov Provider, price float64, volume float64, symbolId int, snap int, quoteTs int) error {

	barTs := n.normalize(quoteTs, snap)
	barKey := prov.FormatBarKey(symbolId, snap, barTs)

	var barInfoString string
	if succ, _ := n.etcdClient.TryGetValue(barKey, &barInfoString); succ {

		barInfo := BarInfo{}
		err := json.Unmarshal([]byte(barInfoString), &barInfo)

		if err != nil {
			return fmt.Errorf("unable to unmarshal BarInfo: %s:: error:: %s",
				barInfoString, err.Error())
		}

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

		return n.PersistBarInfo(barKey, &barInfo)

	} else {
		return n.CreateNewBar(barKey, price, volume, quoteTs, barTs)
	}
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NormalizeSymbols
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (n *Normalizer) NormalizeSymbols(prov Provider, snap int, ts int) error {

	tsPath := prov.FormatTimestampPath(ts)

	_, err := n.etcdClient.EnumerateDirs(tsPath, func(dir string) error {
		dirName := path.Base(dir)
		symbolId, err := strconv.Atoi(dirName)

		if err != nil {
			return fmt.Errorf("dir %s is not convertible to symbolId:: error:: %s",
				dirName, err.Error())
		}

		price, err := prov.GetPrice(ts, symbolId)

		if err != nil {
			return err
		}

		volume, err := prov.GetVolume(ts, symbolId)

		if err != nil {
			return err
		}

		return n.BuildPriceBar(prov, price, volume, symbolId, snap, ts)
	})

	return err
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Do Work
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (n *Normalizer) Normalize(prov Provider) error {
	applog.Infof("start normalizing %s data", prov.Name())

	quotesPath := prov.GetQuotesPath()
	nCount, err := n.etcdClient.EnumerateDirs(quotesPath, func(dir string) error {
		dirName := path.Base(dir)
		ts, err := strconv.Atoi(dirName)

		if err != nil {
			return fmt.Errorf("dir %s is not convertible to unix timestamp:: error:: %s",
				dirName, err.Error())
		}

		applog.Infof("%s: normalizing ts %d quotes", prov.Name(), ts)

		for _, snap := range n.snapSteps {
			err = n.NormalizeSymbols(prov, snap, ts)

			if err != nil {
				return err
			}
		}

		applog.Infof("%s: normalizing successfull, removing ts %d quotes", prov.Name(), ts)
		err = n.RemoveQuoteData(prov, ts)

		if err != nil {
			return err
		}

		return nil
	})

	applog.Infof("%d timestamps processed", nCount)
	return err
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NewNormalizer
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func NewNormalizer(client *etcd.Client, snapSteps []int) *Normalizer {
	return &Normalizer{etcdClient: client, snapSteps: snapSteps}
}
