package main

import(
	"strconv"
	"fmt"
	"github.com/denkhaus/go-etcd/etcd"
	"bitbucket.org/mendsley/tcgl/applog"
)

type Normalizer struct {
	etcdClient *etcd.Client
	snapSteps  []uint
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (n *Normalizer) normalize(ts int, snap int) int {
	return ts - (ts % snap)
}


/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NormalizeSymbols
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (n *Normalizer) NormalizeSymbols(path string, normTs int) error{

	nCount , err := n.etcdClient.EnumerateDirs(path, func (dir string ) error {
		marketId, err := strconv.Atoi(dir)

		if err != nil{
			return err
		}

		priceKey  := fmt.Sprintf("%s/%n/p",path,marketId)
		volumeKey := fmt.Sprintf("%s/%n/v",path,marketId)

		price, err := n.etcdClient.GetValue(priceKey)

		if err != nil{
			return fmt.Errorf("price key %s is not available:: error:: %s",
			priceKey, err.Error())
		}
		
		return nil
	})

	applog.Infof("%n symbols processed", nCount)
	return err
}
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Do Work
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (n *Normalizer) DoWork(provider string) error{
	applog.Infof("start normalizing %s data", provider)

	basePath := fmt.Sprintf("/mkt/%s/quotes", provider)
	nCount , err := n.etcdClient.EnumerateDirs(basePath, func (dir string ) error {
		ts, err := strconv.Atoi(dir)

		if err != nil{
			return err
		}

		for snap := range n.snapSteps{
			 normTs  := n.normalize(ts, snap)
			 actPath := fmt.Sprintf("%s/%n/",basePath,ts)
 			 err =  n.NormalizeSymbols(actPath, normTs)

			if err != nil{
				return err
		       	}
		}

		return nil
	})

	if err != nil{
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
