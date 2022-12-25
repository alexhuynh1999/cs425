package main

import (
	w "cs425-mp4/structs/worker"
	"time"
)

func main() {
	this := w.NewWorker()
	this.StartServer(":6000")
	for {
		time.Sleep(1)
	}
}
