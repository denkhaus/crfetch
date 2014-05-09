package main

import (
	"bitbucket.org/mendsley/tcgl/applog"
	"fmt"
	"github.com/denkhaus/go-signal"
	"github.com/denkhaus/go-store"
	"github.com/denkhaus/yamlconfig"
	"os"
	"runtime"
	"time"
)

type Application struct {
	ticker     *time.Ticker
	quit       chan bool
	store      *store.Store
	normalizer *Normalizer
	config     *yamlconfig.Config
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Stop Application
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (app *Application) Stop() {
	app.quit <- true
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Init Application
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (app *Application) Init() []error {

	errors := make([]error, 0)
	config := yamlconfig.NewConfig("crfetchrc.yaml")

	if err := config.Load(func(config *yamlconfig.Config) {

		config.SetDefault("snapsteps", []int{60, 300, 600, 1800, 3600, 7200, 14400, 28800, 43200, 86400, 259200, 604800})
		config.SetDefault("fetchactionwaitminutes", 2*time.Minute)
		config.SetDefault("normalizeactionat", 5)
		config.SetDefault("erasesourcequoteswaitminutes", 15)
		config.SetDefault("store:instances", 10)
		config.SetDefault("store:network", "tcp")
		config.SetDefault("store:address", ":6379")
		config.SetDefault("store:password", "")
		config.SetDefault("provider:coinbase:apikey", "")

	}, "", false); err != nil {
		return append(errors, fmt.Errorf("load config error:: %s", err.Error()))
	}

	app.config = config
	app.quit = make(chan bool, 1)

	waitMinutes := config.GetDuration("fetchactionwaitminutes")
	app.ticker = time.NewTicker(waitMinutes)

	if st, err := store.NewStore(
		config.GetInt("store:instances"),
		config.GetString("store:network"),
		config.GetString("store:address"),
		config.GetString("store:address")); err != nil {
		return append(errors, err)
	} else {
		app.store = st
	}

	snapSteps := config.GetIntList("snapsteps")
	app.normalizer = NewNormalizer(app.store, snapSteps)

	errors = append(errors, app.InitProviders()...)

	gosignal.ObserveInterrupt().Then(func(sig os.Signal) {
		applog.Infof("application shutdown requested by %v\n", sig)
		app.Stop()
	})

	return errors
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Run Application
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (app *Application) Run() {

	nRuns := 0
	normAction := app.config.GetInt("normalizeactionat")

	for {
		select {
		case <-app.ticker.C:
			nRuns++
			LogSection("pass %d", nRuns)
			if errors := app.CollectData(); len(errors) > 0 {
				ReportErrors("collect data error", errors)
			}

			if nRuns%normAction == 0 {
				LogSection("start normalizing data")
				if errors := app.Normalize(); len(errors) > 0 {
					ReportErrors("normalize error", errors)
				}
			}
		case <-app.quit:
			LogSection("end application")
			app.ticker.Stop()
			return
		}
	}
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// main
///////////////////////////////////////////////////////////////////////////////////////////////////////
func main() {

	app := &Application{}
	runtime.GOMAXPROCS(runtime.NumCPU())
	LogSection("startup application")
	if errors := app.Init(); len(errors) > 0 {
		ReportErrors("init error", errors)
	} else {
		app.Run()
	}
}
