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

	pb "cs425-mp1/communication"

	"google.golang.org/grpc"
	// import proto file
)

// ip adresses/port numbers
const (
	port              = ":6000" //port this server to listening on
	grepTimeoutSecond = 60
	dialTimeoutSecond = 2
	latency_devisor   = 1000000
	maxMsgSize 	  = 1024 * 1024 * 100
)

var (
	adr_arr = [10]string{
		"172.22.95.22",
		"172.22.157.23",
		"172.22.159.23",
		"172.22.95.23",
		"172.22.157.24",
		"172.22.159.24",
		"172.22.95.24",
		"172.22.157.25",
		"172.22.159.25",
		"172.22.95.25",
	}

	buffer_size = len(adr_arr) * 2
)

type FileSystemServer struct {
	pb.UnimplementedFileSystemServer
}

/*
StartServer

	go routine listening incoming connection

@param:

	p: adress and port number in string to listen to
*/
func StartServer(p string) {
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
	if err := s.Serve(listen); err != nil {
		log.Fatalf("failed to serve %v", err)
	}
	log.Printf("serving@ %s", p)
}

/*
Server_handler

	goroutine sending conection with server, calls grep from server, return response, handle error

@param:

	address: server adress for this goroutine to connect
	toSever: grep request channel from main to this goroutine, and send to server
	toClient: shared response channel between all goroutine
	index: used to determine which toServer channel to close when terminate
	counter_chan: channel to update global response counter in main
*/
func Server_handler(address string, toSever <-chan string, toClient chan<- string, index int, channels []chan string, counter_chan chan<- int) {

	connect, err := grpc.Dial(
		address, 
		grpc.WithInsecure(), 
		grpc.WithTimeout(dialTimeoutSecond*time.Second),
		grpc.WithMaxMsgSize(maxMsgSize),
	)
	defer connect.Close()
	if err != nil {

		close(channels[index])
		channels[index] = nil
		log.Fatalf("Dial error: %v", err)
	} else {
		log.Printf("Dial success to %s", address)
	}

	client := pb.NewFileSystemClient(connect)

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
		log.Printf("grep failed Client: %v", err)
	}
	return &pb.Response{Res: time_string + " " + string(result)}, nil
}

/*
Usage:

	input arguments to grep, do not include "grep", put space between arguments

calls server_handler as client, calls startserver as server, pass in command and reduce response, handle user IO
*/
func main() {
	go StartServer(port)
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
	for {
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
	}
}
