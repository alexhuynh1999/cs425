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
	// !!! REPLACE PORT WITH STANDARD PORT ACROSS ALL VMS !!!
	port := ":7000"
	this := w.NewWorker(port)
	this.StartServer(port)
	//this.SetIp("test_ip")
	// !!! REPLACE LOCALHOST WITH COORDINATOR IP !!!
	coordinator_ip, _ := client.GetActive()

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
			client.GetMembers()

		case "put":
			// Usage: 'put [local_file_name] [sdfs_file_name]'
			local_name := commandArr[1]
			sdfs_name := commandArr[2]
			client.Put(local_name, sdfs_name)

		case "get":
			// Usage: 'get [sdfs_file_name] [local_file_name]'
			sdfs_name := commandArr[1]
			local_name := commandArr[2]
			client.Get(sdfs_name, local_name)

		case "delete":
			// Usage: 'delete [sdfs_file_name]'
			sdfs_name := commandArr[1]
			client.Delete(sdfs_name)

		case "ls":
			// Usage: 'ls [sdfs_file_name]'
			sdfs_name := commandArr[1]
			client.ListFiles(sdfs_name)

		case "store":
			client.Store()

		case "get-versions":
			// Usage: 'get-versions [sdfs_file_name] [local_file_name] [versions]
			sdfs_name := commandArr[1]
			local_name := commandArr[2]
			versions := commandArr[3]
			client.GetVersions(sdfs_name, local_name, versions)
		}
	}
}
