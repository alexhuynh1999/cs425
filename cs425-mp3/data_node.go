package main

import (
	"bufio"
	"context"
	data "cs425/mp/proto/data_node"
	dn "cs425/mp/structs/data_node"
	"cs425/mp/utils"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
)

var (
	in_network = [5]string{
		"172.22.95.22",
		"172.22.157.23",
		"172.22.159.23",
		"172.22.95.23",
		"172.22.157.24",
	}
	leader_port = ":6000"
	data_port   = ":7000"
)

func main() {
	//leader_address := ":6000" // change this to fetch from one of four data nodes
	var leader_address string
	for _, machine := range in_network {
		if machine == utils.GetOutboundIP() {
			continue
		}
		address := machine + data_port
		conn, err := grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			log.Printf("[DEBUG] Failed to dial %v", address)
			continue
		}
		log.Printf("[DEBUG] Successfully dialed %v", address)
		client := data.NewDataClient(conn)
		leader_msg, err := client.GetActiveLeader(context.Background(), &data.Empty{})
		if err != nil {
			log.Printf("[DEBUG] %v is not online", address)
			continue
		}
		leader_address = leader_msg.GetLeader()
		break
	}

	log.Printf("[DEBUG] Active leader address: %v", leader_address)

	stream, err := dn.ConnectToLeader(context.Background(), leader_address)
	if err != nil {
		log.Fatalf("[DEBUG] Failed to connect to leader")
	}

	this_machine := dn.NewDataNode(leader_address)
	this_machine.StartDataServer()
	this_machine.BeginHeartbeating(stream)
	this_machine.ListenToLeader(stream)

	time.Sleep(time.Second)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		scanner.Scan()
		commands := scanner.Text()
		commandArr := strings.Fields(commands)

		switch commandArr[0] {
		case "leaders":
			fmt.Println(this_machine.GetMembers())
		case "exit":
			os.Exit(1)
		case "ls":
			fmt.Println(this_machine.DisplayLocalFiles())
		case "query":
			fmt.Println(this_machine.QueryLeader(commands[6:]))
		}
	}
}
