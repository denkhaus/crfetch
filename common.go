package main

import (
	"bitbucket.org/mendsley/tcgl/applog"
	"fmt"
	"github.com/coreos/go-etcd/etcd"
	"io/ioutil"
	"net/http"
)

type Provider interface {
	Init(client *etcd.Client) error
	CollectData() error
}

var providers = make(map[string]Provider)

// Register makes a data Provider available by the provider name.
// If Register is called twice with the same name or if provider is nil,
// it panics.
func RegisterProvider(name string, provider Provider) {

	if provider == nil {
		panic("data: Register provider is nil")
	}

	if _, dup := providers[name]; dup {
		panic("data: Register called twice for provider " + name)
	}
	providers[name] = provider
}

func ReportErrors(prefix string, errors []error) {
	//  err := range errors {
	//      applog.Errorf("%s: %s", prefix, err.Error())
	//  }
}

func FetchData(url string) ([]byte, error) {
	resp, err := http.Get(url)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

func (app *Application) InitProviders() []error {

	errors := make([]error, 0)

	applog.Infof("init dataproviders")
	for name, prov := range providers {

		err := prov.Init(app.etcdClient)

		if err != nil {
			errors = append(errors,
				fmt.Errorf("provider %s :: %s", name, err.Error()))
		}
	}

	return errors
}

func (app *Application) CollectData() []error {

	errors := make([]error, 0)

	applog.Infof("collect data")
	for name, prov := range providers {

		err := prov.CollectData()

		if err != nil {
			errors = append(errors,
				fmt.Errorf("provider %s :: %s", name, err.Error()))
		}
	}

	return errors
}
