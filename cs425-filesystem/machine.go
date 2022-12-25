package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	pb "mp/FileSystem"

	"google.golang.org/grpc"
	// import proto file
)

const (
	//port              = ":6000" //port this server to listening on
	grepTimeoutSecond           = 60
	dialTimeoutSecond           = 2
	pingTimeoutMili             = 1000
	latency_devisor             = 1000000
	maxMsgSize                  = 1024 * 1024 * 100
	pingintervalmili            = 500
	shutServerClientGapTimemili = 500
)

var ( // NEED IP + PORT !!!
	adr_arr = [14]string{
		"172.22.95.22:6000",
		"172.22.157.23:6000",
		"172.22.159.23:6000",
		"172.22.95.23:6000",
		"172.22.157.24:6000",
		"172.22.159.24:6000",
		"172.22.95.24:6000",
		"172.22.157.25:6000",
		"172.22.159.25:6000",
		"172.22.95.25:6000",
		":6000",
		":6100",
		":6200",
		":6300",
	}

	port                       = ":6000"
	buffer_size                = 128
	toConnectionHandler_buffer = 1280
	toFailDetect_buffer        = 1280

	introducerIp = "172.22.95.22:6000" // NEED CHANGE AFTER TESTING !!!

	toConnectionHandler chan string
	toFailDetect        chan string
	MemberFieldSize     = 5
)

type FileSystemServer struct {
	pb.UnimplementedFileSystemServer
}

type Membership struct {
	id                int
	ip                string
	local_time        int64
	list_version_send int
	status            string
}

/*
StartServer

	go routine listening incoming connection

@param:

	p: adress and port number in string to listen to
	signalChan: channal from main, currently close server if recieve a message
*/
func StartServer(p string, signalChan <-chan string) {
	errChan := make(chan error)

	listen, err := net.Listen("tcp", p)
	if err != nil {
		log.Fatalf("listening error: %v", err)
	}
	s := grpc.NewServer(
		grpc.MaxMsgSize(maxMsgSize),
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
	)
	pb.RegisterFileSystemServer(s, &FileSystemServer{})
	log.Printf("listening successful @ %v", listen.Addr())

	go func() {
		if err := s.Serve(listen); err != nil {
			log.Fatalf("failed to serve %v", err)
			errChan <- err
		}
	}()

	defer func() {
		close(errChan)
		s.GracefulStop()
	}()

	select { // close channal if any case is true
	case <-signalChan:

	case <-errChan:
	}

}

/*
Server_handler

	goroutine sending conection with server, ping server or call grep by Faildetection / grepcommand service
	terminate by closing the toSever channel

@param:

	address: server adress for this goroutine to connect
	toSever: grep request channel from communicationhandler to this goroutine, and send to server
	toClient: shared response channel between all to communicationhandler
	targetId: id of the coresponding member to the membershiplist, corrently no use, MIGHT NEED DEBUG
	myIdex: index of the coresponding member to the membershiplist
*/
// target Id is target's Id, myIdex is targe's position in my membershipList
func Server_handler(address string, toSever <-chan string, toClient chan<- string, targetId int, myIdex int) {

	connect, err := grpc.Dial(
		address,
		grpc.WithInsecure(),
		grpc.WithTimeout(dialTimeoutSecond*time.Second),
		grpc.WithMaxMsgSize(maxMsgSize),
	)
	defer connect.Close()
	if err != nil {

		toConnectionHandler <- "dead " + strconv.Itoa(myIdex)
		log.Fatalf("Dial error: %v", err)
	} else {
		log.Printf("Dial success to %s", address)
	}

	client := pb.NewFileSystemClient(connect)

	for val := range toSever {

		command, command_args := getType(val)

		switch command {
		case "ping":
			ctx, cancel := context.WithTimeout(context.Background(), pingTimeoutMili*time.Millisecond)
			defer cancel()

			//pong can be used later, no use now
			pong, err := client.FailDetect(ctx, &pb.Ping{Ping: []string{"ping", command_args}})
			if err != nil {
				//log.Printf("dead " + strconv.Itoa(myIdex) + "\n")
				toConnectionHandler <- "dead " + strconv.Itoa(myIdex)
				continue
			}
			//log.Printf("pinging " + strconv.Itoa(myIdex) + "\n")
			toClient <- "pong " + strconv.Itoa(myIdex) + " " + pong.GetPong()[1]
		case "pingList":
			ctx, cancel := context.WithTimeout(context.Background(), pingTimeoutMili*time.Millisecond)
			defer cancel()

			pong, err := client.FailDetect(ctx, &pb.Ping{Ping: []string{"pingList", command_args}})
			if err != nil {
				//log.Printf("dead " + strconv.Itoa(myIdex) + "\n")
				toConnectionHandler <- "dead " + strconv.Itoa(myIdex)
				continue
			}
			//log.Printf("pinglist " + strconv.Itoa(myIdex) + "\n")
			toClient <- "pong " + strconv.Itoa(myIdex) + " " + pong.GetPong()[1]
		case "join":
			ctx, cancel := context.WithTimeout(context.Background(), pingTimeoutMili*time.Millisecond)
			defer cancel()
			pong, err := client.FailDetect(ctx, &pb.Ping{Ping: []string{"pingJoin", command_args}})
			if err != nil {
				//log.Printf("dead " + strconv.Itoa(myIdex) + "\n")
				toConnectionHandler <- "dead " + strconv.Itoa(myIdex)
				continue
			}
			//log.Printf("pingJoin " + strconv.Itoa(myIdex) + "\n")
			toClient <- "pong " + strconv.Itoa(myIdex) + " " + pong.GetPong()[1]

		case "grep":
			ctx, cancel := context.WithTimeout(context.Background(), grepTimeoutSecond*time.Second)
			defer cancel()
			res, err := client.GrepReq(ctx, &pb.Request{Req: string(command_args)})
			if err != nil {
				log.Printf("grep failed Server: %v", err)
				toConnectionHandler <- "grepFail " + strconv.Itoa(myIdex)
			} else {
				log.Printf("grep success from: %s \n", address)
				toClient <- "grepResult " + res.GetRes()
			}

		}

	}

	/* MP1 code
	//keep listening on toServer channel, send request return response or handle connection fail
	//time.Sleep(5 * time.Second) // delete! for swtiching funciton testing
	for req := range toSever {
		func() { //wrap to allow defer cancel for individual req
			ctx, cancel := context.WithTimeout(context.Background(), grepTimeoutSecond*time.Second)
			defer cancel()
			//	req := "'hello' sample.txt\n"
			res, err := client.GrepReq(ctx, &pb.Request{Req: string(req)})
			if err != nil {
				close(channels[index])
				channels[index] = nil
				counter_chan <- 1
				log.Printf("grep failed Server: %v", err)
			} else {
				log.Printf("grep success from: %s \n", address)
				toClient <- res.GetRes() + "from vm" + strconv.Itoa(index+1) + ".log@" + address + " "
				counter_chan <- 2
			}
		}()
	}
	*/
}

/*
GrepReq

	server grep function, needs future update
	server function call:  strip time, call exec.command(grep), and return time + result

@param

	ctx: currently no use, can add feature to propagate timeout to server
	req: argument to grep as Req {string}, string = time + command

	return: result from grep Res {string}
*/
func (s *FileSystemServer) GrepReq(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	args := strings.Fields(req.GetReq())
	time_string := args[0]
	args = args[1:len(args)]

	// Algorithm to determine when a pattern starts
	strIdx := -1
	patternStarted := false
	pattern := ""
	for idx, arg := range args {
		hasQuote := strings.Contains(arg, "\"")

		if !patternStarted && hasQuote {
			patternStarted = true
			pattern += arg[1:len(arg)]
			strIdx = idx
		} else if patternStarted {
			pattern += " " + arg
		}

		if idx == len(args)-2 {
			break
		}
	}
	var execArgs []string
	if strIdx == -1 {
		execArgs = args
	} else {
		for idx, arg := range args {
			if idx == strIdx {
				execArgs = append(execArgs, pattern)
				execArgs = append(execArgs, args[len(args)-1])
				break
			} else {
				execArgs = append(execArgs, arg)
			}
		}
	}

	for len(execArgs[0]) == 13 {
		execArgs = execArgs[1:len(execArgs)]
	}
	result, err := exec.Command("grep", execArgs...).Output()
	if err != nil {
		//log.Printf("grep failed Client: %v", err)
	}
	return &pb.Response{Res: time_string + " " + string(result)}, nil
}

/*
FailDetect

	server pong function, pong with local timestemp

@param

	ctx: currently no use
	Ping: ping message send by server_handler from other machine

@return

	pong message containing format label timestemp
*/
func (s *FileSystemServer) FailDetect(ctx context.Context, ping *pb.Ping) (*pb.Pong, error) {
	args := ping.GetPing()
	command := args[0]
	switch command {
	case "pingList":
		toConnectionHandler <- "newList " + args[1]
		//log.Printf("pong list\n")
		return &pb.Pong{Pong: []string{"pong", strconv.FormatInt(time.Now().Unix(), 10)}}, nil

	case "pingJoin":
		toConnectionHandler <- "join_request " + args[1]
		//log.Printf("pong join\n")
		return &pb.Pong{Pong: []string{"pong", strconv.FormatInt(time.Now().Unix(), 10)}}, nil

	default:
		return &pb.Pong{Pong: []string{"pong", strconv.FormatInt(time.Now().Unix(), 10)}}, nil
	}

}

/*
membershipString

	turn membership array to a string with space seperator
*/
func membershipString(membershipMap []Membership) string {
	ret := ""
	for _, v := range membershipMap {
		ret += " " + strconv.Itoa(v.id) + " " + v.ip + " " + strconv.FormatInt(v.local_time, 10) + " " + strconv.Itoa(v.id) + " " + v.status + "\n"
	}
	return ret
}

/*
membershipStringToList

	turn membership string to a list, currently have no use, MIGHT NEED DEBUG
*/
func membershipStringToList(s string) []Membership {
	args := strings.Fields(s)
	i := 0
	membershipList := []Membership{}
	for i < len(args) {
		income_id, _ := strconv.Atoi(args[i])
		income_ip := args[1]
		income_time, _ := strconv.ParseInt(args[i+2], 10, 64)
		income_version, _ := strconv.Atoi(args[i+3])
		income_status := args[i+4]
		membershipList = append(membershipList, Membership{id: income_id, ip: income_ip, local_time: income_time, list_version_send: income_version, status: income_status})

		i += MemberFieldSize
	}
	return membershipList
}

/*
getType

	first return a substring before the first space, the second return is the rest of the string,
	delete newline at the end if there is one
*/
func getType(args string) (string, string) {
	if args[len(args)-1] == '\n' {
		args = args[:len(args)-1]
	}
	command_type_index := strings.Index(args, " ")
	command := ""
	command_args := ""
	if command_type_index == -1 {
		command = args
	} else {
		command = args[:command_type_index]
		if len(args) > command_type_index+1 {
			command_args = args[command_type_index+1:]
		}
	}
	return command, command_args
}

/*
nextRound

	compute the next round-robin index
*/
func nextRound(index int, length int) int {
	if index >= length-1 {
		return 0
	} else {
		return index + 1
	}
}

/*
ConnectionHandler

	client side code, keep listening on toConnection channel, communicate with main and server_handler
	update memberhsiplist, add/delete channels to serverhandler, create new member ...
	pings live members in membershiplist periodically, listen on channel if not pinging

@param

	toConnection: channel listening if not pinging, shared by all go routines, equal to global var toConnectionHandler
	fromConnection: channel send result to main
	myIp: ip adress of this machine
*/
func ConnectionHandler(toConnection <-chan string, fromConnection chan<- string, myIp string) {
	timeChan := time.NewTicker(time.Millisecond * pingintervalmili).C
	ping_index := 0
	var membership_list []Membership
	list_version := 0
	server_handler_map := make(map[int](chan string))
	//dirtyMap := 0

	for {
		select {
		case input := <-toConnection:
			input_type, input_args := getType(input)

			switch input_type {
			//membership_list = append(membership_list, Membership{id: len(membership_list), ip: myIp, local_time: time.Now().Unix(), status: "a", last_version_send: 0})
			case "newList":
				input_args_arr := strings.Fields(input_args)
				i := 0
				isModified := 0
				for { // break until end of list
					is_found := 0
					income_id, _ := strconv.Atoi(input_args_arr[i])
					income_time, _ := strconv.ParseInt(input_args_arr[i+2], 10, 64)
					//income_version, _ := strconv.Atoi(input_args_arr[i+3])
					income_status := input_args_arr[i+4]
					for i, _ := range membership_list {

						if membership_list[i].id == income_id {
							is_found = 1

							if income_status == "d" && membership_list[i].status != "d" { // close channel if dead
								membership_list[i].status = "d"
								isModified = 1
								//log.Printf("closingchannelID%d\n", income_id)
								close(server_handler_map[income_id])
							}

							if income_time > membership_list[i].local_time && membership_list[i].status != "d" { // replace
								//isModified = 1
								membership_list[i].local_time = income_time
								membership_list[i].status = income_status
							}
						}
						if is_found == 1 {
							break
						}
					}
					if is_found == 0 { // new income member, append
						//log.Printf("incomenewNode: %d\n", income_id)
						isModified = 1
						income_ip := input_args_arr[i+1]
						membership_list = append(membership_list, Membership{id: income_id, ip: income_ip, local_time: income_time, status: income_status})
						if income_status == "a" {
							new_chan := make(chan string, buffer_size)
							server_handler_map[income_id] = new_chan
							server_handler_map[income_id] = new_chan
							go Server_handler(income_ip, new_chan, toConnectionHandler, income_id, len(membership_list)-1)
						}
					}

					i += MemberFieldSize // skip id and ip, NEED to change if membership filds length modified !
					if i >= len(input_args_arr) {
						break
					}
				}
				if isModified == 1 {
					list_version += 1
				}

			case "introduce":
				membership_list = append(membership_list, Membership{id: 0, ip: myIp, local_time: time.Now().Unix(), status: "a"})
				new_chan := make(chan string, buffer_size)
				server_handler_map[0] = new_chan
				go Server_handler(introducerIp, new_chan, toConnectionHandler, 0, 0)
				
			case "join": //joining to network
				new_chan := make(chan string, buffer_size)
				server_handler_map[0] = new_chan
				membership_list = append(membership_list, Membership{id: 0, ip: introducerIp, local_time: 0, list_version_send: 0, status: "a"})
				go Server_handler(introducerIp, new_chan, toConnectionHandler, 0, 0) // no need for index, becuase Connection handler close chnnel after updating server_handler_map/memberhsip_list
				new_chan <- "join " + myIp

			case "join_request": // listener side request to join, because this is introducer
				new_ip := input_args
				new_chan := make(chan string, buffer_size)
				new_id := membership_list[len(membership_list)-1].id + 1 // most recent id +1
				membership_list = append(membership_list, Membership{id: new_id, ip: new_ip, list_version_send: list_version, status: "a"})
				server_handler_map[new_id] = new_chan
				go Server_handler(new_ip, new_chan, toConnectionHandler, new_id, len(membership_list)-1) // no need for index, becuase Connection handler close chnnel after updating server_handler_map/memberhsip_list
				join_ping_mes := "pingList " + membershipString(membership_list)
				new_chan <- join_ping_mes

			case "reqMembership":
				fromConnection <- membershipString(membership_list)

			case "leave":
				for _, m := range membership_list {
					if m.status == "a" {
						close(server_handler_map[m.id])
					}
				}

				membership_list = make([]Membership, 0)

			case "pong":
				index_s, ltime := getType(input_args)
				index_i, _ := strconv.Atoi(index_s)
				ltime_64, _ := strconv.ParseInt(ltime, 10, 64)
				membership_list[index_i].local_time = ltime_64

			case "dead":
				index, _ := strconv.Atoi(input_args)
				if membership_list[index].status == "d" {
					log.Printf("\n WARNING: super dead!! \n")
					continue
				}
				membership_list[index].status = "d"
				list_version += 1
				//log.Printf("closing channel ID%d\n", membership_list[index].id)
				//close(server_handler_map[membership_list[index].id])
			}

		case <-timeChan: //routine err check
			// sip if empty list or I'm the only one on list
			if len(membership_list) == 0 || (len(membership_list) == 1 && membership_list[0].ip == myIp) {
				continue
			}

			ping_index = nextRound(ping_index, len(membership_list))

			//don't ping myself & bounds check
			dead_count := 0
			all_dead := false
			for membership_list[ping_index].status == "d" || membership_list[ping_index].ip == myIp {
				dead_count += 1
				if dead_count >= len(membership_list) { // continue if everyone else is dead
					//log.Printf("allDead\n")
					all_dead = true
					break
				}
				ping_index = nextRound(ping_index, len(membership_list))
			}
			if all_dead {
				break
			}

			pingMessage := ""
			if membership_list[ping_index].list_version_send < list_version {
				pingMessage = "pingList " + membershipString(membership_list)
				membership_list[ping_index].list_version_send = list_version
			} else {
				pingMessage = "ping"
			}

			server_handler_map[membership_list[ping_index].id] <- pingMessage
		}
	}
}

/*
user interface to process user commands and send processed commands to CommunicationHandler

Usage:

	select ip to select ip adress of this machine
	introduce the introducer first
	join none introducer
	membership to display membership list
	leave to leave group without terminate
	rejoin to rejoin the group with a new id
*/
func main() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("\nSelect VM: ")
	args, _ := reader.ReadString('\n')
	idx, _ := getType(args)
	ipIdx, _ := strconv.Atoi(idx)
	myIp := adr_arr[ipIdx]
	isInNetwork := 0
	grpcSeverSignalChan := make(chan string)
	go StartServer(myIp, grpcSeverSignalChan)
	toConnectionHandler = make(chan string, toConnectionHandler_buffer) // need larger buffer, all connect through this
	toFailDetect = make(chan string, toFailDetect_buffer)
	fromConnectionHandler := make(chan string, buffer_size)

	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("\ncommand: ")
		args, _ := reader.ReadString('\n')
		//args := strings.Fields(command)
		if len(args) == 0 {
			fmt.Print("please type in command here: \n")
			continue
		}

		// get first argument by space
		command, command_args := getType(args)

		switch command {
		case "selectIp":
			ip_index, err := strconv.Atoi(command_args)
			if err == nil {
				if ip_index < len(adr_arr) {
					myIp = adr_arr[ip_index]
				}
			} else {
				log.Printf("err: myIp")
			}

		case "introduce":

			go ConnectionHandler(toConnectionHandler, fromConnectionHandler, myIp)
			isInNetwork = 1
			toConnectionHandler <- "introduce"

		case "join":
			if isInNetwork == 1 {
				log.Printf("already joined network\n")
				continue
			}
			isInNetwork = 1
			go ConnectionHandler(toConnectionHandler, fromConnectionHandler, myIp)
			toConnectionHandler <- "join " + introducerIp + " " + myIp

		case "membership":
			if isInNetwork == 0 {
				log.Printf("not yet joined network\n")
				continue
			}
			toConnectionHandler <- "reqMembership"
			list := <-fromConnectionHandler
			log.Printf("\n" + list + "\n")

		case "leave":
			grpcSeverSignalChan <- "leave"                             // no matter what input, it will terminate
			time.Sleep(shutServerClientGapTimemili * time.Millisecond) // make sure server shutdown before emptying membershiplist
			toConnectionHandler <- "leave"
			close(grpcSeverSignalChan)
			isInNetwork = 0

		case "rejoin":
			grpcSeverSignalChan = make(chan string)
			go StartServer(myIp, grpcSeverSignalChan)

			if isInNetwork == 1 {
				log.Printf("already joined network\n")
				continue
			}
			isInNetwork = 1

			toConnectionHandler <- "join " + introducerIp + " " + myIp

		case "ip":
			fmt.Printf("%s\n",myIp)

		default:
			fmt.Printf("unkonw command\n")
		}

		// rest have not been modified, need modify later
		/*
			counter_chan := make(chan int, buffer_size)            // update counter channel
			toClient := make(chan string, buffer_size)             // shared reposne channel
			toServer_channels := make([]chan string, len(adr_arr)) // individual request channel
			for i := 0; i < len(adr_arr); i++ {
				toServer_channels[i] = make(chan string, 2)
			}
			//setting up connection concurrently
			for index, adr := range adr_arr {
				adr = adr + port
				go Server_handler(adr, toServer_channels[index], toClient, index, toServer_channels, counter_chan)
			}
			// request loop
			time.Sleep(200 * time.Millisecond) // wait for server to turn on

			// need change to if command ==

			if command == "grep" {
				reader := bufio.NewReader(os.Stdin)
				fmt.Print("\ngrep ")
				command, _ := reader.ReadString('\n')
				request_count := 0 // =0 when #requeest == #response, so we can move on to reducing
				valid_count := 0   // how many valid reponse, +2 if valid, +1 if not, count == #valid,used to tell when we are done reducing

				//send request
				for _, channel := range toServer_channels {
					if channel == nil {
						continue
					}
					valid_count -= 1
					request_count -= 1
					command = strconv.Itoa(int(time.Now().UnixNano()/latency_devisor)) + " " + command // time in milisecond
					channel <- command
				}
				//wait for response
				if request_count != 0 {
					for i := range counter_chan {
						valid_count += i
						request_count += 1
						if request_count == 0 {
							break
						}
					}
				}
				//reduce response
				ret_string := ""
				sum := 0
				for i := 0; i < valid_count; i++ {
					temp := <-toClient
					now_time := int(time.Now().UnixNano() / latency_devisor)
					temp = string(temp)
					tempWords := strings.Fields(temp)
					lineCount := tempWords[1]
					tempInt, err := strconv.Atoi(lineCount)
					if err != nil {
					} else {
						sum += tempInt
					}

					send_time := ""
					for idx, c := range temp {
						if c == ' ' {
							send_time = temp[:idx]
							temp = temp[idx+1 : len(temp)]
							break
						}
					}
					send_time_int, _ := strconv.Atoi(send_time)
					diff_time := now_time - send_time_int
					diff_time_string := "delay in milisec: " + strconv.Itoa(diff_time) + "\n"

					ret_string = ret_string + temp + diff_time_string
				}

				fmt.Print(ret_string)
				fmt.Printf("Total line count: %d\n", sum)

				continue
			}
		*/
	}
}
