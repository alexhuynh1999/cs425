package main

import (
	"bufio"
	dn "cs425/mp/structs/data_node"
	ln "cs425/mp/structs/leader_node"
	utils "cs425/mp/utils"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func Run() {
	// change when testing on vm
	my_ip := utils.GetOutboundIP()

	leader := ln.NewLeaderNode(
		[]string{my_ip},
		1,
		[]string{my_ip},
		1,
		my_ip,
	)
	leader.StartLeaderServer(":6000")
	// set active flag?
	leader.SetActive()

	this_machine := dn.NewDataNode(my_ip)
	this_machine.StartDataServer()

	time.Sleep(time.Second)
	scanner := bufio.NewScanner(os.Stdin)
	for {
		scanner.Scan()
		commands := scanner.Text()
		commandArr := strings.Fields(commands)
		if len(commandArr) == 0 {
			fmt.Printf("Please type in command: [Usage]\n members leaders exit \n" +
				"put get delete lsAdr lsVer store get-versions\n ")
			continue
		}
		switch commandArr[0] {
		case "members":
			fmt.Println(leader.MembershipList.GetVersion())
			fmt.Println(leader.MembershipList.GetMembers())
		case "leaders":
			fmt.Println(leader.LeaderList.GetVersion())
			fmt.Println(leader.LeaderList.GetMembers())
		case "exit":
			os.Exit(1)
		case "put":
			if len(commandArr) != 4 {
				fmt.Println("Usage: put [localfilename] [sdfsfilename] [version]")
			} else {
				WriteFileSuccess := leader.WriteFile(commandArr[1], commandArr[2], commandArr[3])
				if !WriteFileSuccess {
					fmt.Println("WriteFileFailed")
				}
			}
		case "get":
			if len(commandArr) != 4 {
				fmt.Println("Usage: get [sdfsfilename] [version] [localfilename]")
			} else {
				file, _ := leader.ReadFile(commandArr[1], commandArr[2])
				leader.WriteToLocal(commandArr[3], file)
			}
		case "delete":
			if len(commandArr) == 2 { // delete all version
				leader.DeleteFileEveryVersion(commandArr[1])
			} else if len(commandArr) == 3 { // delete one version
				leader.DeleteFileVersion(commandArr[1], commandArr[2])
			} else {
				fmt.Println("Usage: delete [sdfsfilename] [version], or delete all version with only [sdfsfilename]")
			}
		case "lsAdr":
			if len(commandArr) != 3 {
				fmt.Println("Usage: ls [sdfsfilename] [version]")
			} else {
				fmt.Println(leader.ListFileReplicaAdr(commandArr[1], commandArr[2]))
			}
		case "get-versions":
			if len(commandArr) != 4 {
				fmt.Println("Usage: get-versions [sdfsfilename] [how many versions] [local filename]")
			} else {
				howMany, err := strconv.Atoi(commandArr[2])
				if err != nil {
					fmt.Println("Usage: get-versions [sdfsfilename] [how many versions]  [local filename]")
				} else {
					file := leader.ReadLatestVersions(commandArr[1], howMany)
					leader.WriteToLocal(commandArr[3], file)
				}
			}
		case "lsVer":
			if len(commandArr) != 2 {
				fmt.Println("Usage: lsVer [sdfsfilename]")
			} else {
				result, exist := leader.FileList.GetAllVersions(commandArr[1])
				if !exist {
					fmt.Printf("%s does not exist\n", commandArr[1])
				} else {
					fmt.Println(result)
				}
			}

		default:
			fmt.Println("invalid command")
		}
	}
}

func main() {
	Run()
}
