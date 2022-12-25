package main

import (
	"time"

	c "cs425-mp4/structs/coordinator"
)

var (
	coordinator_port = ":6000"
)

func main() {
	this := c.NewCoordinator()
	this.StartServer(coordinator_port)
	for {
		time.Sleep(1)
	}
}
