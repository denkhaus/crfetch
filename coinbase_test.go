package main

import (
	"fmt"
	"github.com/denkhaus/go-store"
	"testing"
)

func createProvider(t *testing.T) (c *CoinbaseProvider) {

	machines := []string{}
	etcdClient := etcd.NewClient(machines)

	provider := &CoinbaseProvider{}
	provider.Init(etcdClient)

	//if c.APIKey == "" {
	//	t.Skip("Coinbase api key is missing (should be in the COINBASE_API_KEY environment variable)")
	//}

	return provider
}

func TestGetMarketIdBySymbol(t *testing.T) {
	provider := createProvider(t)

	mktid, err := provider.GetMarketIdBySymbol("btc_to_usd")

	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("mktid for btc_to_usd is %+v\n", mktid)
}

/*
func TestAccountReceiveAddress(t *testing.T) {
	c := createAccountClient(t)

	address, err := c.GetAccountReceiveAddress()
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("%+v\n", address)
}

func TestAccountGenerateReceiveAddress(t *testing.T) {

		c := createAccountClient(t)

		address, err := c.GenerateAccountReceiveAddress("")

		if err != nil {
			t.Fatal(err)
		}

		fmt.Printf("%+v\n", address)

		address, err = c.GenerateAccountReceiveAddress("http://www.example.com/")

		if err != nil {
			t.Fatal(err)
		}

		fmt.Printf("%+v\n", address)

}
*/
