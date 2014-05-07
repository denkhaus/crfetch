package main

import (
	"bitbucket.org/mendsley/tcgl/applog"
	"bytes"
	"fmt"
	"github.com/denkhaus/go-store"
	"github.com/denkhaus/yamlconfig"
	"io/ioutil"
	"net/http"
)

type Provider interface {
	Init(config *yamlconfig.Config, store *store.Store) error
	CollectData() error
	FormatBarKey(snap int, barTs int) string
	FormatBarHash(symbolId int) string
	EnumerateQuotes(enumQuotesFunc EnumQuotesFunc) error
	RemoveQuotes() error
	Name() string
}

var providers = make(map[string]Provider)

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Register makes a data Provider available by the provider name.
// If Register is called twice with the same name or if provider is nil,
// it panics.
///////////////////////////////////////////////////////////////////////////////////////////////////////
func RegisterProvider(name string, provider Provider) {

	if provider == nil {
		panic("data: Register provider is nil")
	}

	if _, dup := providers[name]; dup {
		panic("data: Register called twice for provider " + name)
	}
	providers[name] = provider
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Report Errors
///////////////////////////////////////////////////////////////////////////////////////////////////////
func LogSection(format string, args ...interface{}) {

	var segString bytes.Buffer
	txt := fmt.Sprintf(format, args...)

	for seg := (100 - len(txt)) / 2; seg > 0; seg-- {
		segString.WriteString("=")
	}

	fmt.Printf("\n%s %s %s\n\n", segString.String(), txt, segString.String())
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Report Errors
///////////////////////////////////////////////////////////////////////////////////////////////////////
func ReportErrors(prefix string, errors []error) {
	for _, err := range errors {
		applog.Errorf("%s: %s", prefix, err.Error())
	}
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Fetch Data
///////////////////////////////////////////////////////////////////////////////////////////////////////
func FetchData(url string) ([]byte, error) {
	resp, err := http.Get(url)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Init Providers
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (app *Application) InitProviders() (errors []error) {
	applog.Infof("init dataproviders")

	errors = make([]error, 0)
	for name, prov := range providers {
		if err := prov.Init(app.config, app.store); err != nil {
			errors = append(errors,
				fmt.Errorf("provider %s :: %s", name, err.Error()))
		}
	}

	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Normalize
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (app *Application) Normalize() (errors []error) {
	applog.Infof("normalize data")

	errors = make([]error, 0)
	for name, provider := range providers {
		err := app.normalizer.Normalize(provider)

		if err != nil {
			errors = append(errors,
				fmt.Errorf("provider %s :: %s", name, err.Error()))
		}
	}

	return
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Collect Data
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (app *Application) CollectData() (errors []error) {
	applog.Infof("collect data")

	errors = make([]error, 0)
	for name, prov := range providers {

		err := prov.CollectData()

		if err != nil {
			errors = append(errors,
				fmt.Errorf("provider %s :: %s", name, err.Error()))
		}
	}

	return
}
