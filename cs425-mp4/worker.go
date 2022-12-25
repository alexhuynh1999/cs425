package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	client "cs425-mp4/structs/client_commands"
	w "cs425-mp4/structs/worker"
)

var (
	worker_port = ":7000"
)

func main() {
	coordinator_ip, _ := client.GetActive()
	this := w.NewWorker(worker_port)
	this.SetActive(coordinator_ip)
	this.StartServer(worker_port)

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

		case "get-active":
			fmt.Printf("Active coordinator: %v\n", this.GetCoordinator())

		case "train":
			// Usage: `train [batch_size]`
			batch_size, err := strconv.Atoi(commandArr[1])
			if err != nil {
				fmt.Print("Invalid batch size. Try again")
			} else {
				int_batch := int32(batch_size)
				client.Train(int_batch)
			}

		case "inference":
			// Usage: `inference [num_queries]`
			num_queries, err := strconv.Atoi(commandArr[1])
			if err != nil {
				fmt.Print("Invalid number of queries entered. Try again")
			} else {
				n_queries := int32(num_queries)
				client.Inference(n_queries)
			}

		case "get-rate":
			// Usage: `get-rate [model_id]` - DEPRECATED
			client.GetRate()

		case "stats":
			// Usage: `stats [model_id]`
			model_id, err := strconv.Atoi(commandArr[1])
			if err != nil {
				fmt.Print("Invalid model id. Try again")
			} else {
				model_id := int32(model_id)
				client.Stats(model_id)
			}

		case "set-batch-size":
			// Usage: `set-batch-size [model_id] [batch_size]`
			model_id, err1 := strconv.Atoi(commandArr[1])
			batch_size, err2 := strconv.Atoi(commandArr[2])
			if err1 != nil {
				fmt.Print("Invalid model id. Try again")
			} else if err2 != nil {
				fmt.Print("Invalid batch size. Try again")
			} else {
				model_id := int32(model_id)
				batch_size := int32(batch_size)
				client.SetBatchSize(model_id, batch_size)
			}

		case "ls-jobs":
			client.ShowJobs()

		case "get-results":
			client.GetQuery()
		}
	}
}
