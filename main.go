package main

import (
	"bitbucket.org/mendsley/tcgl/applog"
	"fmt"
	"github.com/denkhaus/go-etcd/etcd"
	"github.com/denkhaus/yamlconfig"
	"os"
	"os/signal"
	"time"
)

var Configuration = &yamlconfig.Config{}

type Application struct {
	ticker     *time.Ticker
	quit       chan bool
	etcdClient *etcd.Client
	normalizer *Normalizer
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Stop Application
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (app *Application) Stop() {
	app.quit <- true
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Register interrupt handlers so we can stop the ethereum
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (app *Application) RegisterInterupts() {
	// Buffered chan of one is enough
	c := make(chan os.Signal, 1)

	// Notify about interrupts for now
	signal.Notify(c, os.Interrupt)

	go func() {
		for sig := range c {
			applog.Infof("application shutdown requested by %v\n", sig)
			app.Stop()
		}
	}()
}

func (app *Application) LoadDefaults(config *Config) {
	config.SetDefault("snapsteps", []int{60, 300, 600, 1800, 3600, 7200, 14400, 28800, 43200, 86400, 259200, 604800})
	config.SetDefault("fetchactionwaitminutes", 2)
	config.SetDefault("erasesourcequoteswaitminutes", 15)
	config.SetDefault("etcd:machines", []string{})
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Init Application
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (app *Application) Init() []error {

	errors := make([]error, 0)

	if err := Configuration.Load(app.LoadDefaults, "", false); err != nil {
		return append(errors, fmt.Errorf("load config error:: %s", err.Error()))
	}

	app.quit = make(chan bool, 1)

	waitMinutes := Configuration.GetInt("fetchactionwaitminutes")
	app.ticker = time.NewTicker(waitMinutes * time.Minute)

	machines := config.GetStringList("etcd:machines")
	app.etcdClient = etcd.NewClient(machines)

	snapSteps := Configuration.GetIntList("snapsteps")
	app.normalizer = NewNormalizer(app.etcdClient, snapSteps)

	errors = append(errors, app.InitProviders()...)
	app.RegisterInterupts()

	return errors
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Run Application
///////////////////////////////////////////////////////////////////////////////////////////////////////
func (app *Application) Run() {

	nRuns := 0

	for {
		select {
		case <-app.ticker.C:
			nRuns++
			LogSection("pass %d", nRuns)

			if errors := app.CollectData(); len(errors) > 0 {
				ReportErrors("collect data error", errors)
			}

			if nRuns%5 == 0 {
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
	LogSection("startup application")
	if errors := app.Init(); len(errors) > 0 {
		ReportErrors("init error", errors)
	} else {
		app.Run()
	}
}
