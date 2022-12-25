package main

import (
	"time"

	c "cs425-mp4/structs/coordinator"
	w "cs425-mp4/structs/worker"
)

var (
	coordinator_port = ":6000"
	worker_port      = ":7000"
)

func main() {
	coordinator := c.NewCoordinator()
	coordinator.StartServer(coordinator_port)
	coordinator.StandbyHealthCheck()
	worker := w.NewWorker(worker_port)
	worker.SetActive(coordinator.Address)
	worker.StartServer(worker_port)

	for {
		time.Sleep(1)
	}
}
