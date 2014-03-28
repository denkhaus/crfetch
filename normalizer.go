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
	o float64
	h float64
	l float64
	c float64
	v float64
	t int
}

type BarInfo struct {
	data  BarData
	tsMin int
	tsMax int
}

type Normalizer struct {
	etcdClient *etcd.Client
	snapSteps  []uint
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (b *BarData) Initialize(price float64, volume float64, timestamp int) {
	b.o = price
	b.h = price
	b.l = price
	b.c = price
	b.v = volume
	b.t = timestamp
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// normalize
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (n *Normalizer) normalize(ts int, snap int) int {
	return ts - (ts % snap)
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
	barData.Initialize(price, volume, barTs)
	barInfo := BarInfo{data: barData, tsMax: quoteTs, tsMin: quoteTs}

	return n.PersistBarInfo(key, &barInfo)
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// BuildBar
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (n *Normalizer) BuildPriceBar(tsPath string, price float64, volume float64, symbolId int, snap int, quoteTs int) error {

	providerPath := path.Dir(tsPath)
	barTs := n.normalize(quoteTs, snap)
	barKey := fmt.Sprintf("%s/bars/%n/%n/%n", providerPath, symbolId, snap, barTs)

	var barInfoString string
	if succ, _ := n.etcdClient.TryGetValue(barKey, &barInfoString); succ {

		barInfo := BarInfo{}
		err := json.Unmarshal([]byte(barInfoString), &barInfo)

		if err != nil {
			return fmt.Errorf("unable to unmarshal BarInfo: %s:: error:: %s",
				barInfoString, err.Error())
		}

		if quoteTs < barInfo.tsMin {
			barInfo.tsMin = quoteTs
			barInfo.data.o = price
		}

		if quoteTs > barInfo.tsMax {
			barInfo.tsMax = quoteTs
			barInfo.data.c = price
		}

		if price > barInfo.data.h {
			barInfo.data.h = price
		}

		if price < barInfo.data.l {
			barInfo.data.l = price
		}

		return n.PersistBarInfo(barKey, &barInfo)

	} else {
		return n.CreateNewBar(barKey, price, volume, quoteTs, barTs)
	}
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NormalizeSymbols
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (n *Normalizer) NormalizeSymbols(path string, snap int, ts int) error {

	applog.Debugf("normalizing all data for timestamp %n by snap %n", ts, snap)
	nCount, err := n.etcdClient.EnumerateDirs(path, func(dir string) error {
		symbolId, err := strconv.Atoi(dir)

		if err != nil {
			return fmt.Errorf("dir %s is not convertible to symbolId:: error:: %s",
				dir, err.Error())
		}

		priceKey := fmt.Sprintf("%s/%n/p", path, symbolId)
		volumeKey := fmt.Sprintf("%s/%n/v", path, symbolId)

		priceString, err := n.etcdClient.GetValue(priceKey)

		if err != nil {
			return fmt.Errorf("price key %s is not available:: error:: %s",
				priceKey, err.Error())
		}

		var volume float64
		if volumeString, _ := n.etcdClient.GetValue(volumeKey); len(volumeString) > 0 {
			volume, err = strconv.ParseFloat(volumeString, 64)

			if err != nil {
				return fmt.Errorf("unable to parse volume data %s to float64:: error:: %s",
					volumeString, err.Error())
			}
		} else {
			volume = 0.0
		}

		price, err := strconv.ParseFloat(priceString, 64)

		if err != nil {
			return fmt.Errorf("unable to parse price data %s to float64:: error:: %s",
				priceString, err.Error())
		}

		return n.BuildPriceBar(path, price, volume, symbolId, snap, ts)
	})

	applog.Infof("%n symbols processed", nCount)
	return err
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Do Work
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (n *Normalizer) DoWork(provider string) error {
	applog.Infof("start normalizing %s data", provider)

	basePath := fmt.Sprintf("/mkt/%s/quotes", provider)
	nCount, err := n.etcdClient.EnumerateDirs(basePath, func(dir string) error {
		ts, err := strconv.Atoi(dir)

		if err != nil {
			return fmt.Errorf("dir %s is not convertible to unix timestamp:: error:: %s",
				dir, err.Error())
		}

		for snap := range n.snapSteps {

			actPath := fmt.Sprintf("%s/%n", basePath, ts)
			err = n.NormalizeSymbols(actPath, snap, ts)

			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	applog.Infof("%n timestamps processed", nCount)
	return nil
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NewNormalizer
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func NewNormalizer(client *etcd.Client, snapSteps []uint)(* Normalizer) {
	return &Normalizer{etcdClient : client, snapSteps: snapSteps}
}