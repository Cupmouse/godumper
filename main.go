package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/exchangedataset/godumper/dumper"
)

func main() {
	logger := log.New(os.Stdout, "godumper", log.LstdFlags)

	if len(os.Args) < 3 {
		logger.Fatal("please specify an exchange, and directory")
	}

	// choose dumper for the exchange provided
	exchange := os.Args[1]
	directory := os.Args[2]

	// receive SIGINT and setup to do stuff before exiting
	// putting this will disable default behavior to immediately exit the application
	// instead ignore the signal
	interruptSignal := make(chan os.Signal)
	signal.Notify(interruptSignal, os.Interrupt)

	// main loop to endlessly dump
	for {
		logger.Println("starting dumper...")
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
	}
}
