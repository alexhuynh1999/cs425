package main

import (
	"bufio"
	"log"
	"os"
	"strings"
	"time"

	client "cs425-mp4/structs/client_commands"
	w "cs425-mp4/structs/worker"
)

func main() {
	port := ":7000"
	this := w.NewWorker(port)
	this.StartServer(":7000")
	//this.SetIp("test_ip")
	// !!! REPLACE LOCALHOST WITH COORDINATOR IP !!!
	coordinator_ip := "localhost"

	time.Sleep(time.Second)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		scanner.Scan()
		command := scanner.Text()
		commandArr := strings.Fields(command)

		switch commandArr[0] {
		case "join":
			if err := this.Join(coordinator_ip); err != nil {
				log.Printf("[FAIL] Unable to join network")
			}

		case "list_mem":
			client.GetMembers(coordinator_ip)
		}
	}
}
