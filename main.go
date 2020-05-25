package main

import (
	"log"
	"math"
	"os"
	"os/signal"
	"time"

	"github.com/exchangedataset/godumper/dumper"
)

// RapidRestartThreshold is the time threshold that if dumper has restarted within this period of time
// from the last restarted time is considered as rapid restart.
// Too much rapid restarts will make the main loop to limit attempts to reconnect by delaying it
const RapidRestartThreshold = 60 * time.Second

// RestartWaitTimeLimit is the limit to restart wait time. Main loop will not attempt delaying retry
// more than this
const RestartWaitTimeLimit = 60 * time.Second

func main() {
	logger := log.New(os.Stdout, "godumper", log.LstdFlags)

	if len(os.Args) < 3 {
		logger.Fatal("please specify an exchange, and directory")
	}

	// choose dumper for the exchange provided
	exchange := os.Args[1]
	directory := os.Args[2]

	// main loop to endlessly dump
	recentRestartCount := 0
	for {
		logger.Println("starting dumper...")
		lastStartTime := time.Now()

		// receive SIGINT and setup to do stuff before exiting
		// putting this will disable default behavior to immediately exit the application
		// instead ignore the signal
		interruptSignal := make(chan os.Signal)
		signal.Notify(interruptSignal, os.Interrupt)

		errCh := make(chan error)
		// buffering one is important
		// this will ensure sending to this channel won't block main loop
		stop := make(chan bool, 1)
		go dumper.Dump(exchange, directory, logger, errCh, stop)

		select {
		case dumpErr := <-errCh:
			logger.Println("error occurred:", dumpErr)
			logger.Println("restarting...")

			for {
				dumpErr, ok := <-errCh
				if !ok {
					break
				}
				logger.Println("error occurred:", dumpErr)
			}
			break
		case <-interruptSignal:
			// received SIGINT, exit the program
			logger.Println("received SIGINT, exiting...")
			// send stop signal to dumper
			stop <- true
			// wait for the dumper thread to stop
			for {
				dumpErr, ok := <-errCh
				if !ok {
					// channel closed
					logger.Println("exiting")
					os.Exit(1)
				}
				logger.Println("error occurred:", dumpErr)
			}
		}

		// unnotify receiving interrupt
		signal.Reset(os.Interrupt)

		// it has to restart, but might have to wait for a little
		if time.Now().Sub(lastStartTime) <= RapidRestartThreshold {
			// this restart is counted as a rapid restart
			waitTime := time.Duration(math.Pow(2, float64(recentRestartCount))) * time.Second
			if waitTime >= RestartWaitTimeLimit {
				waitTime = RestartWaitTimeLimit
			}
			logger.Printf("rapid restart detected, waiting for %d seconds...\n", waitTime/time.Second)
			time.Sleep(waitTime)
		} else {
			// not a rapid restart, reset the count to 0
			recentRestartCount = 0
		}
		recentRestartCount++
	}
}
