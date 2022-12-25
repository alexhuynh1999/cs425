package main

import (
	w "cs425-mp4/structs/worker"
	"time"
)

func main() {
	port := ":6000"
	this := w.NewWorker(port)
	this.StartServer(port)
	for {
		time.Sleep(1)
	}
}
