package data_node

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	data "cs425/mp/proto/data_node"
	lead "cs425/mp/proto/leader"
	ln "cs425/mp/structs/leader_node"
	ml "cs425/mp/structs/membership_list"
	"cs425/mp/utils"

	"google.golang.org/grpc"
)

type DataNode struct {
	data.UnimplementedDataServer
	Ip           string
	LeaderList   ml.MembershipList
	ActiveLeader string
}

var (
	leader_port = ":6000"
	data_port   = ":7000"
)

func (d *DataNode) StartDataServer() {
	go func() {
		// replace with actual address
		lis, err := net.Listen("tcp", data_port)
		if err != nil {
			log.Fatalf("[FAIL] Unable to listen on port: 7000")
		}
		server := grpc.NewServer()
		data.RegisterDataServer(server, d)
		log.Print("[DEBUG] Serving from DataNode server")
		if err := server.Serve(lis); err != nil {
			log.Fatalf("[FAIL] Unable to serve: %v", err)
		}
	}()
}

func NewDataNode(leader_address string) DataNode {
	rand.Seed(time.Now().UnixNano())
	//ip := strconv.Itoa(rand.Intn(1000) + 1)
	ip := utils.GetOutboundIP()

	this_machine := DataNode{
		// replace with actual IP of machine
		Ip:           ip,
		LeaderList:   ml.CreateMembershipList([]string{ip}, 0),
		ActiveLeader: leader_address,
	}

	return this_machine
}

func (d *DataNode) BeginHeartbeating(stream lead.Leader_JoinClient) {
	go func() {
		if d.heartbeat(stream) != nil {
			log.Printf("[FAIL] Unable to heartbeat to leader")
		}
		for {
			if d.heartbeat(stream) != nil {
				log.Printf("[WARNING] Unable to heartbeat to leader")
				return
			}
			time.Sleep(time.Second)
		}
	}()
}

func (d *DataNode) heartbeat(stream lead.Leader_JoinClient) error {
	hb := lead.Hb{
		Heartbeat:             true,
		MembershipListVersion: d.LeaderList.GetVersion(),
		Ip:                    d.GetIp(),
	}
	//log.Print("[DEBUG] Heartbeat to leader")
	return stream.Send(&hb)
}

func (d *DataNode) GetMembers() []string {
	return d.LeaderList.GetMembers()
}

func (d *DataNode) GetIp() string {
	return d.Ip
}

func (d *DataNode) ListenToLeader(stream lead.Leader_JoinClient) {
	go func() {
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				log.Printf("[WARNING] Lost connection to leader: %v", err)
				return
			}
			if err != nil {
				log.Printf("[WARNING] Lost connection to leader: %v", err)
				return
			} else {
				d.LeaderList = ml.CreateMembershipList(
					resp.GetMembers(),
					resp.GetVersion(),
				)
				log.Print("[DEBUG] Received member list")
			}
		}
	}()
}

func (d *DataNode) Promote(ctx context.Context, in *data.NetworkMembers) (*data.Ack, error) {
	log.Print("[DEBUG] Beginning promotion")
	leader := ln.NewLeaderNode(
		in.GetMembers(),
		in.GetMemberVersion(),
		in.GetLeaders(),
		in.GetLeaderVersion(),
		d.GetIp(),
	)
	leader.StartLeaderServer(leader_port)
	stream, err := ln.ConnectToLeaderStandby(context.Background(), d.ActiveLeader+leader_port)
	if err != nil {
		log.Printf("[DEBUG] Promotion error: Failed to connect to active leader")
		return &data.Ack{Done: true}, err
	}
	leader.BeginHeartbeating(stream)
	leader.ListenToLeader(stream, d.ActiveLeader)

	return &data.Ack{Done: true}, nil
}

func (d *DataNode) UpdateActiveLeader(ctx context.Context, in *data.Leader) (*data.Ack, error) {
	address := in.GetLeader()
	stream, err := ConnectToLeader(ctx, address)
	if err != nil {
		log.Printf("[DEBUG] Failed to update active leader")
		return &data.Ack{Done: true}, err
	}
	d.BeginHeartbeating(stream)
	d.ActiveLeader = address
	//d.ListenToLeader(stream)

	return &data.Ack{Done: true}, nil
}

func (d *DataNode) GetActiveLeader(ctx context.Context, in *data.Empty) (*data.Leader, error) {
	return &data.Leader{Leader: d.ActiveLeader}, nil
}

func (d *DataNode) Read(ctx context.Context, readName *data.ReadName) (*data.FileContent, error) {
	bytes, err := os.ReadFile(readName.FileName)
	if err != nil {
		log.Printf("[ERROR] failed to read file, FileName: %s", readName.FileName)
		return &data.FileContent{FileContent: ""}, fmt.Errorf("Data: failed to locate, FileName: %s", readName.FileName)
	}
	return &data.FileContent{FileContent: string(bytes)}, nil
}

func (d *DataNode) Write(ctx context.Context, fileToWrite *data.WriteFile) (*data.Ack, error) {
	f, err := os.Create(fileToWrite.FileName)
	if err != nil {
		log.Printf("[ERROR] failed to create file, FileName: %s", fileToWrite.FileName)
		return &data.Ack{Done: false}, fmt.Errorf("[ERROR] failed to create file, FileName: %s", fileToWrite.FileName)
	}
	defer f.Close()

	_, err = f.WriteString(fileToWrite.FileContent)
	if err != nil {
		log.Printf("[ERROR] failed to write to file, FileName: %s", fileToWrite.FileName)
		return &data.Ack{Done: false}, fmt.Errorf("[ERROR] failed to write to file, FileName: %s", fileToWrite.FileName)
	}
	return &data.Ack{Done: true}, nil
}

func (d *DataNode) Delete(ctx context.Context, fileToDelete *data.DeleteFile) (*data.Ack, error) {
	err := os.Remove(fileToDelete.FileName)
	if err != nil {
		log.Printf("[ERROR] failed to delete file, FileName: %s", fileToDelete.FileName)
		return &data.Ack{Done: false}, fmt.Errorf("[ERROR] failed to delete file, FileName: %s", fileToDelete.FileName)
	}
	return &data.Ack{Done: true}, nil
}

func (d *DataNode) DisplayLocalFiles() []string {

	//d.ActiveLeader = ":6000" // !!!!!delete this !!!!!

	conn, err := grpc.Dial(d.ActiveLeader, grpc.WithInsecure())
	if err != nil {
		log.Printf("[WARNING] Unable to dial leader: %v\n", err)
		return nil
	}
	client := lead.NewLeaderClient(conn)
	resp, err := client.ShowFileInDataNode(context.Background(), &lead.NodeAddress{Address: d.Ip})
	if err != nil {
		log.Printf("[ERROR] DisplayLocalFiles: %s\n", err)
		return nil
	}
	return resp.LocalFileNames
}

func ConnectToLeader(ctx context.Context, leader string) (lead.Leader_JoinClient, error) {
	conn, err := grpc.Dial(leader+leader_port, grpc.WithInsecure())
	if err != nil {
		log.Printf("[DEBUG] [WARNING] Failed to connect to leader %v", leader)
		return nil, err
	}

	client := lead.NewLeaderClient(conn)
	stream, err := client.Join(ctx)
	if err != nil {
		log.Printf("[FAIL] Unable to connect to stream DATA: %v", err)
		return nil, err
	}
	return stream, nil
}

func (d *DataNode) QueryLeader(outCommand string) string {
	conn, err := grpc.Dial(d.ActiveLeader, grpc.WithInsecure())
	if err != nil {
		log.Printf("[WARNING] Unable to dial leader: %v\n", err)
		return ""
	}
	client := lead.NewLeaderClient(conn)
	resp, err := client.Query(context.Background(), &lead.GeneralMessage{Message: outCommand})
	if err != nil {
		log.Printf("QueryLeader failed, err: %s\n", err)
	}

	commandArr := strings.Fields(outCommand)
	if commandArr[0] == "get" || commandArr[0] == "get-versions" {
		localFileName := commandArr[3]

		f, err := os.Create(localFileName)
		if err != nil {
			log.Printf("[ERROR] QueryLeader: failed to create file, FileName: %s", localFileName)
		}
		defer f.Close()

		_, err = f.WriteString(resp.Message)
		if err != nil {
			log.Printf("[ERROR] QueryLeader: failed to write to file, FileName: %s", localFileName)
		}
	}
	return resp.Message
}
