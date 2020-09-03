package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/exchangedataset/streamcommons"
)

// RapidRestartThreshold is the time threshold that if dumper has restarted within this period of time
// from the last restarted time is considered as rapid restart.
// Too much rapid restarts will make the main loop to limit attempts to reconnect by delaying it
const RapidRestartThreshold = 60 * time.Second

// RestartWaitTimeLimit is the limit to restart wait time. Main loop will not attempt delaying retry
// more than this
const RestartWaitTimeLimit = 60 * time.Second

func main() {
	exchange := flag.String("exchange", "", "a name of target exchange")
	directory := flag.String("directory", "./dumpfiles", "path to the directory to store dumps")
	alwaysDisk := flag.Bool("disk", true, "always store dumps as file")
	prod := flag.Bool("prod", false, "turn on production mode")
	flag.Parse()
	logger := log.New(os.Stdout, "godumper", log.LstdFlags)

	var dumpFunc func(context.Context, string, bool, *log.Logger) error
	switch *exchange {
	case "bitmex":
		dumpFunc = dumpBitmex
	case "bitflyer":
		dumpFunc = dumpBitflyer
	case "bitfinex":
		dumpFunc = dumpBitfinex
	case "binance":
		dumpFunc = dumpBinance
	case "bitbank":
		logger.Println("dump for bitbank is not mantained")
		dumpFunc = dumpBitbank
	case "liquid":
		dumpFunc = dumpLiquid
	default:
		fmt.Fprintln(os.Stderr, "Specify an exchange")
		os.Exit(1)
	}

	if *prod {
		// Enable
		streamcommons.AWSEnableProduction()
		a := false
		alwaysDisk = &a
	}

	// main loop to endlessly dump
	recentRestartCount := 0
	for {
		logger.Println("starting dumper...")
		lastStartTime := time.Now()

		// Receive SIGINT and setup to do stuff before exiting.
		// Putting this will disable default behavior to immediately exit the application,
		// instead ignore the signal.
		interruptSignal := make(chan os.Signal)
		signal.Notify(interruptSignal, os.Interrupt, syscall.SIGTERM)
		ctx, cancel := context.WithCancel(context.Background())
		dumpErr := make(chan error)
		go func() {
			serr := dumpFunc(ctx, *directory, *alwaysDisk, logger)
			if serr != nil {
				dumpErr <- serr
			}
			close(dumpErr)
		}()
		select {
		case serr, ok := <-dumpErr:
			if ok {
				logger.Println("error occurred:", serr)
			}
			logger.Println("restarting...")
			break
		case <-interruptSignal:
			// Received SIGINT, exit the program
			logger.Println("received SIGINT, exiting...")
			// Send stop signal to dumper
			cancel()
			// Wait for the dumper thread to stop
			err, ok := <-dumpErr
			if ok {
				logger.Println(err)
			}
			// channel closed
			logger.Println("exiting")
			os.Exit(1)
		}
		// Unnotify receiving interrupt
		signal.Reset(os.Interrupt)
		// Wait for dump routine to stop
		<-dumpErr
		// Restart, but might have to wait for a little
		if time.Now().Sub(lastStartTime) <= RapidRestartThreshold {
			// this restart is counted as a rapid restart
			waitTime := time.Duration(math.Pow(2, float64(recentRestartCount))) * time.Second
			if waitTime >= RestartWaitTimeLimit {
				waitTime = RestartWaitTimeLimit
			}
			logger.Printf("rapid restart detected, waiting for %d seconds...\n", waitTime/time.Second)
			time.Sleep(waitTime)
		} else {
			// Not a rapid restart, reset the count to 0
			recentRestartCount = 0
		}
		recentRestartCount++
	}
}
