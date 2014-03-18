package main

import (
	"bitbucket.org/mendsley/tcgl/applog"
	"github.com/coreos/go-etcd/etcd"
	"os"
	"os/signal"
	"time"
)

type Application struct {
	ticker     *time.Ticker
	quit       chan bool
	etcdClient *etcd.Client
}

func (app *Application) Stop() {
	app.quit <- true
}

// Register interrupt handlers so we can stop the ethereum
func (app *Application) RegisterInterupts() {
	// Buffered chan of one is enough
	c := make(chan os.Signal, 1)
	// Notify about interrupts for now
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			applog.Infof("Shutting down (%v) ... \n", sig)
			app.Stop()
		}
	}()
}

func (app *Application) Init() []error {

	errors := make([]error, 0)

	app.quit = make(chan bool, 1)
	app.ticker = time.NewTicker(2 * time.Minute)
	app.RegisterInterupts()

	machines := []string{}
	app.etcdClient = etcd.NewClient(machines)

	errors = append(errors, app.InitProviders()...)
	return errors
}

func (app *Application) Run() {

	for {
		select {
		case <-app.ticker.C:
			if errors := app.CollectData(); len(errors) > 0 {
				ReportErrors("collect error", errors)
			}
		case <-app.quit:
			app.ticker.Stop()
			return
		}
	}
}

func main() {

	app := &Application{}
	if errors := app.Init(); len(errors) > 0 {
		ReportErrors("init error", errors)
	} else {
		app.Run()
	}
}
