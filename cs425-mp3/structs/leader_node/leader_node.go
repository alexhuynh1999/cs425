package leader_node

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	data "cs425/mp/proto/data_node"
	lead "cs425/mp/proto/leader"
	fl "cs425/mp/structs"
	ml "cs425/mp/structs/membership_list"
	utils "cs425/mp/utils"

	"google.golang.org/grpc"
)

var (
	leader_port = ":6000"
	data_port   = ":7000"
)

const MaxFailure = 0

type LeaderNode struct {
	lead.UnimplementedLeaderServer
	MembershipList ml.MembershipList
	LeaderList     ml.MembershipList
	Ip             string
	Status         string
	lastestId      uint // each file's unique id, ever increasing +=1
	FileList       fl.FileList
}

func (leader *LeaderNode) GetIp() string {
	return leader.Ip
}

func (leader *LeaderNode) Join(server lead.Leader_JoinServer) error {
	req, err := server.Recv()
	if err != nil {
		log.Printf("Received error %v", err)
	}
	request_ip := req.GetIp()
	log.Printf("[DEBUG] Adding %v to the network", request_ip)
	leader.MembershipList.Add(request_ip)
	ctx := server.Context()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		req, err := server.Recv()
		if err == io.EOF {
			log.Println("No more messages in stream.")
			return nil
		}
		if err != nil {
			log.Printf("[DEBUG] Deleting %v from the network", request_ip)
			leader.MembershipList.Delete(request_ip)
			leader.LeaderList.Delete(request_ip)
			// request_ip is DEAD, rereplicate
			leader.Rereplication(request_ip)

			continue
		}

		if req.GetMembershipListVersion() < leader.MembershipList.GetVersion() {
			if leader.SendMemberList(server, request_ip) != nil {
				log.Print("[WARNING] Unable to update MembershipList")
			}
		}
	}
}

func (leader *LeaderNode) JoinStandby(server lead.Leader_JoinStandbyServer) error {
	req, err := server.Recv()
	if err != nil {
		log.Printf("Received error %v", err)
	}
	request_ip := req.GetIp()
	log.Printf("[DEBUG] Adding %v to the leader list", request_ip)
	leader.LeaderList.Add(request_ip)
	ctx := server.Context()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		req, err := server.Recv()
		if err == io.EOF {
			log.Println("No more messages in stream.")
			return nil
		}
		if err != nil {
			log.Printf("[DEBUG] Deleting %v from the network", request_ip)
			leader.LeaderList.Delete(request_ip)
			continue
		}

		if (req.GetMembershipListVersion() < leader.MembershipList.GetVersion()) ||
			(req.GetLeaderListVersion() < leader.LeaderList.GetVersion()) {
			if leader.SendStandby(server, request_ip) != nil {
				log.Print("[WARNING] Unable to update MembershipList")
			}
		}
	}
}

func (leader *LeaderNode) SendMemberList(server lead.Leader_JoinServer, request_ip string) error {
	log.Printf("[DEBUG] Sending updated MembershipList to %v", request_ip)
	msg := lead.MemberList{
		Members: leader.MembershipList.GetMembers(),
		Version: leader.MembershipList.GetVersion(),
	}
	return server.Send(&msg)
}

func (leader *LeaderNode) SendStandby(server lead.Leader_JoinStandbyServer, request_ip string) error {
	log.Printf("[DEBUG] Sending updated MembershipList to %v", request_ip)
	msg := lead.NetworkMembers{
		Members:               leader.MembershipList.GetMembers(),
		MembershipListVersion: leader.MembershipList.GetVersion(),
		Leaders:               leader.LeaderList.GetMembers(),
		LeaderListVersion:     leader.LeaderList.GetVersion(),
	}
	return server.Send(&msg)
}

func (leader *LeaderNode) GetAvailableLeader() string {
	available_nodes := utils.GetLeftExclusive(leader.MembershipList.Members, leader.LeaderList.Members)
	if len(available_nodes) == 0 {
		return ""
	}
	out := available_nodes[rand.Intn(len(available_nodes))]
	return out
}

func (leader *LeaderNode) InvokeDataNode(address string, command string) {
	conn, err := grpc.Dial(address+data_port, grpc.WithInsecure())
	if err != nil {
		log.Printf("[WARNING] Unable to dial new standby: %v", err)
		return
	}
	client := data.NewDataClient(conn)
	switch command {
	case "Promote":
		msg := data.NetworkMembers{
			Members:       leader.MembershipList.GetMembers(),
			MemberVersion: leader.MembershipList.GetVersion(),
			Leaders:       leader.LeaderList.GetMembers(),
			LeaderVersion: leader.LeaderList.GetVersion(),
		}
		resp, err := client.Promote(context.Background(), &msg)
		if err != nil {
			log.Printf("[WARNING] Unable to promote DataNode: %v", err)
			return
		} else if resp.Done {
			log.Printf("[DEBUG] Promoted DataNode to Leader")
		}
	case "UpdateActiveLeader":
		msg := data.Leader{
			Leader: leader.GetIp(),
		}
		resp, err := client.UpdateActiveLeader(context.Background(), &msg)
		if err != nil {
			log.Printf("[WARNING] Unable to update active leader of %v: %v", address, err)
			return
		} else if resp.Done {
			log.Printf("[DEBUG] Updated active leader at %v", address)
		}
	}
}

func (leader *LeaderNode) PromoteLeaders() {
	go func() {
		for {
			time.Sleep(time.Second)
			//log.Print("[DEBUG] Leader healthcheck")
			if leader.LeaderList.Length() < 4 {
				new_standby := leader.GetAvailableLeader()
				//log.Printf("[DEBUG] Available standbys: %v", len(new_standby))
				if new_standby == "" {
					//log.Print("[DEBUG] No available nodes for promotion")
					continue
				}
				if new_standby == leader.GetIp() {
					continue
				}
				leader.InvokeDataNode(new_standby, "Promote")
			}
		}
	}()
}

func (leader *LeaderNode) StartLeaderServer(port string) {
	go func() {
		lis, err := net.Listen("tcp", port)
		if err != nil {
			log.Printf("[FAIL] Unable to listen on port: %v", port)
			log.Printf("%v", err)
			return
		}

		server := grpc.NewServer()
		lead.RegisterLeaderServer(server, leader)
		log.Print("[DEBUG] Serving from LeaderNode server")
		if err := server.Serve(lis); err != nil {
			log.Fatalf("[FAIL] Unable to serve: %v", err)
		}
	}()
}

func (leader *LeaderNode) SetActive() {
	log.Print("[DEBUG] Setting as active leader of network")
	for _, address := range leader.MembershipList.GetMembers() {
		// update member's active leader
		leader.InvokeDataNode(address, "UpdateActiveLeader")
	}

	leader.Status = "active"
	leader.PromoteLeaders()
}

func NewLeaderNode(members []string, member_version int32, leaders []string, leader_version int32, ip string) LeaderNode {
	rand.Seed(time.Now().UnixNano())
	this_machine := LeaderNode{
		MembershipList: ml.CreateMembershipList(members, member_version),
		LeaderList:     ml.CreateMembershipList(leaders, leader_version),
		FileList:       fl.InitFileList(),
		Ip:             ip,
	}

	return this_machine
}

// check if int contained in slice, used for generating unique addresses
func contains(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// return true if success write
func (leader *LeaderNode) WriteFile(localFileName string, remoteFileName string, version string) bool {
	memberAddresses := leader.MembershipList.GetMembers()
	//memberAddresses := []string{":8000"} // !!!!need to change to above!!!!

	bytes, err := os.ReadFile(localFileName)
	if err != nil {
		log.Printf("[ERROR] failed to read local file, FileName: %s\n", localFileName)
		return false
	}

	var addedAddress []string // address of main and replicas
	leader.lastestId += 1
	thisId := leader.lastestId
	var fileIds []uint
	fileIds = append(fileIds, thisId)
	var addedNames []string
	successAdd := leader.FileList.AddTranslation(remoteFileName, version, thisId)
	if !successAdd {
		return false
	}
	remoteFileName = leader.FileList.Translate(remoteFileName, version)
	if len(remoteFileName) == 0 {
		log.Printf("WriteFile: translateError: %s + %s\n", remoteFileName, version)
	}
	addedNames = append(addedNames, remoteFileName)
	var randIdx []int

	successGeneration := 0 // generate unique random index for memberAddresses
	for successGeneration != MaxFailure+1 {
		newRand := rand.Intn(len(memberAddresses))
		if !contains(randIdx, newRand) {
			randIdx = append(randIdx, newRand)
			successGeneration += 1
		} else {
			continue
		}
	}

	for _, Idx := range randIdx {
		conn, err := grpc.Dial(memberAddresses[Idx], grpc.WithInsecure())
		if err != nil {
			log.Printf("[WARNING] Unable to dial writing node: %v\n", err)
			return false
		}
		client := data.NewDataClient(conn)
		resp, err := client.Write(context.Background(), &data.WriteFile{FileName: remoteFileName, FileContent: string(bytes)})
		if err != nil {
			log.Printf("[ERROR] Leader: writing failed to addr %s\n", memberAddresses[Idx])
			return false
		}
		if resp.Done != true {
			log.Printf("[ERROR] Leader: DataNode: writing failed addr: %s\n", memberAddresses[Idx])
			return false
		}
		addedAddress = append(addedAddress, memberAddresses[Idx])
	}
	leader.FileList.Add(addedNames, addedAddress, fileIds)
	return true
}

// bool = false if all fail, else bool = true
func (leader *LeaderNode) ReadFile(fileName string, version string) (string, bool) {
	fileName = leader.FileList.Translate(fileName, version)
	if len(fileName) == 0 {
		log.Printf("ReadFile: translateError: %s + %s\n", fileName, version)
		return "", false
	}

	replicasAdr := leader.FileList.GetFile(fileName)
	if len(replicasAdr) == 0 { // empty string
		log.Printf("Leader: Failed to locate file, FileName: %s\n", fileName)
		return "", false
	}
	for _, adr := range replicasAdr {
		conn, err := grpc.Dial(adr, grpc.WithInsecure())
		if err != nil {
			log.Printf("[WARNING] Unable to dial reading node: %v\n", err)
			continue
		}
		client := data.NewDataClient(conn)
		resp, err := client.Read(context.Background(), &data.ReadName{FileName: fileName})
		if err != nil {
			log.Printf("ReadFile: err from read in Data: %s\n", err)
			continue
		}
		return resp.FileContent, true
	}
	log.Printf("[ERROR] Leader: could not get from any replica, FileName: %s\n", fileName)
	return "", false
}

// delete one version of the file, return true if file exist, false if it's non existent
func (leader *LeaderNode) DeleteFileVersion(originalFileName string, version string) bool {
	fileName := leader.FileList.Translate(originalFileName, version)
	if len(fileName) == 0 {
		log.Printf("DeleteFileVersion: translateError: %s + %s\n", fileName, version)
		return false
	}

	deletedAddresses := leader.FileList.DeleteFile(fileName)
	if len(deletedAddresses) == 0 {
		log.Printf("Leader: delete nonexistent file, FileName: %s\n", fileName)
		return false
	}
	for _, adr := range deletedAddresses {
		conn, err := grpc.Dial(adr, grpc.WithInsecure())
		if err != nil {
			log.Printf("[WARNING] Unable to dial deleting node: %v\n", err)
			continue
		}
		client := data.NewDataClient(conn)
		client.Delete(context.Background(), &data.DeleteFile{FileName: fileName})
		if err != nil {
			log.Printf("Leader: DataNode deleting error: %s\n", err)
		}
	}
	leader.FileList.DeleteTranslationOneVersion(originalFileName, version)
	return true
}

func (leader *LeaderNode) DeleteFileEveryVersion(fileName string) bool {
	versions, exist := leader.FileList.GetAllVersions(fileName)
	if !exist {
		log.Printf("DeleteFileEveryVersion: GetAllVersions: can't find fileName: %s\n", fileName)
		return false
	}
	for _, version := range versions {
		success := leader.DeleteFileVersion(fileName, version)
		if !success {
			log.Printf("DeleteFileEveryVersion: DeleteVersion unsuccessful: %s + %s\n", fileName, version)
			return false
		}
	}
	return true
}

// read multiple versions and concat to one string
func (leader *LeaderNode) ReadLatestVersions(fileName string, howMany int) string {
	ret := ""
	latestVersions, success1 := leader.FileList.GetLatestVersions(fileName, howMany)
	if !success1 {
		log.Printf("ReadLatestVersions: GetLatestVersions unsuccessful, Name: %s, count %d\n", fileName, howMany)
	}
	for _, version := range latestVersions {
		result, success2 := leader.ReadFile(fileName, version)
		if !success2 {
			log.Printf("ReadLatestVersions: ReadFile unsuccessful, Name: %s, version %s\n", fileName, version)
			continue
		}
		ret = ret + "\n\n" + "version:" + version + "\n" + result
	}
	return ret
}

func (leader *LeaderNode) ListFileReplicaAdr(fileName string, version string) string {
	ret := ""
	translatedFileName := leader.FileList.Translate(fileName, version)
	if len(fileName) == 0 {
		log.Printf("Leader: translateError: %s + %s\n", fileName, version)
	}
	addresses := leader.FileList.ShowReplicas(translatedFileName)
	for _, adr := range addresses {
		ret = ret + adr + "\n"
	}
	return ret
}

func (leader *LeaderNode) WriteToLocal(fileName string, content string) {
	f, err := os.Create(fileName)
	if err != nil {
		log.Printf("[ERROR] WriteLocal: failed to create file, FileName: %s", fileName)
		return
	}
	defer f.Close()

	_, err = f.WriteString(content)
	if err != nil {
		log.Printf("[ERROR] WriteLocal: failed to write to file, FileName: %s", fileName)
	}
}

func (leader *LeaderNode) ShowFileInDataNode(ctx context.Context, AddressGRPC *lead.NodeAddress) (*lead.LocalFileNames, error) {
	address := AddressGRPC.Address
	names := leader.FileList.ShowFileInAddress(address)
	return &lead.LocalFileNames{LocalFileNames: names}, nil
}

// false if not enough alive node to replicate, or all node fail
func (leader *LeaderNode) Rereplication(failedAddress string) bool {
	fileNames, fileIds := leader.FileList.DeleteNode(failedAddress)

	for i, name := range fileNames {
		done := false
		// try read from alive replication
		for aliveAdr, _ := range leader.FileList.FileToAdr[name] {
			conn, err := grpc.Dial(aliveAdr, grpc.WithInsecure())
			if err != nil {
				log.Printf("[WARNING] Rereplication: Unable to dial reading node: %v\n", err)
				continue
			}
			client := data.NewDataClient(conn)
			fileContentGrpc, err := client.Read(context.Background(), &data.ReadName{FileName: name})
			if err != nil {
				log.Printf("Rereplication: err from read data: %s\n", err)
				continue
			}

			// re replicate
			memberAddresses := leader.MembershipList.GetMembers()
			if len(memberAddresses) < MaxFailure+1 {
				log.Printf("Rereplication: not enough node alive")
				return false
			}
			for { // try get an unoccupied node for this file
				newRand := rand.Intn(len(memberAddresses))
				if _, exist := leader.FileList.FileToAdr[name][memberAddresses[newRand]]; exist {
					continue
				}

				conn, err = grpc.Dial(memberAddresses[newRand], grpc.WithInsecure())
				if err != nil {
					log.Printf("Rereplication Unable to dial writing node: %v\n", err)
					time.Sleep(time.Second) // !!!!! delete this, no need to wait, just choose the next random node
					continue
				}
				client = data.NewDataClient(conn)
				_, err1 := client.Write(context.Background(), &data.WriteFile{FileName: name, FileContent: fileContentGrpc.FileContent})
				if err1 != nil {
					log.Printf("Rereplication: wrtie error: %s", err1)
					time.Sleep(time.Second) // !!!!! delete this, no need to wait, just choose the next random node
					continue
				}
				leader.FileList.Add([]string{name}, []string{memberAddresses[newRand]}, []uint{fileIds[i]})
				done = true
				break
			}
			break
		}
		if !done {
			log.Printf("[Error] Rereplication: all attempt failed on file: %s", name)
			return false
		}
	}
	return true
}

func (leader *LeaderNode) Query(ctx context.Context, queryCommand lead.GeneralMessage) (lead.GeneralMessage, error) {
	ret := ""
	commands := queryCommand.Message
	commandArr := strings.Fields(commands)

	switch commandArr[0] {
	case "put":
		if len(commandArr) != 4 {
			ret = fmt.Sprintf("Usage: put [localfilename] [sdfsfilename] [version]\n")
		} else {
			WriteFileSuccess := leader.WriteFile(commandArr[1], commandArr[2], commandArr[3])
			if !WriteFileSuccess {
				ret = fmt.Sprintf("WriteFileFailed\n")
			}
		}
	case "get":
		if len(commandArr) != 4 {
			ret = fmt.Sprintf("Usage: get [sdfsfilename] [version] [localfilename]\n")
		} else {
			file, _ := leader.ReadFile(commandArr[1], commandArr[2])
			ret = file
		}
	case "delete":
		if len(commandArr) == 2 { // delete all version
			leader.DeleteFileEveryVersion(commandArr[1])
		} else if len(commandArr) == 3 { // delete one version
			leader.DeleteFileVersion(commandArr[1], commandArr[2])
		} else {
			ret = fmt.Sprintf("Usage: delete [sdfsfilename] [version], or delete all version with only [sdfsfilename]\n")
		}
	case "lsAdr":
		if len(commandArr) != 3 {
			ret = fmt.Sprintf("Usage: ls [sdfsfilename] [version]\n")
		} else {
			ret = fmt.Sprintf(leader.ListFileReplicaAdr(commandArr[1], commandArr[2]) + "\n")
		}
	case "get-versions":
		if len(commandArr) != 4 {
			ret = fmt.Sprintf("Usage: get-versions [sdfsfilename] [how many versions] [local filename]\n")
		} else {
			howMany, err := strconv.Atoi(commandArr[2])
			if err != nil {
				ret = fmt.Sprintf("Usage: get-versions [sdfsfilename] [how many versions]  [local filename]\n")
			} else {
				file := leader.ReadLatestVersions(commandArr[1], howMany)
				ret = file
			}
		}
	case "lsVer":
		if len(commandArr) != 2 {
			ret = fmt.Sprintf("Usage: lsVer [sdfsfilename]\n")
		} else {
			result, exist := leader.FileList.GetAllVersions(commandArr[1])
			if !exist {
				ret = fmt.Sprintf("%s does not exist\n", commandArr[1])
			} else {
				ret = fmt.Sprintf(strings.Join(result, " ") + "\n")
			}
		}

	default:
		ret = fmt.Sprintf("invalid command\n")
	}

	return lead.GeneralMessage{Message: ret}, nil
}

func ConnectToLeaderStandby(ctx context.Context, leader string) (lead.Leader_JoinStandbyClient, error) {
	conn, err := grpc.Dial(leader, grpc.WithInsecure())
	if err != nil {
		log.Printf("[DEBUG] [WARNING] Failed to connect to leader %v", leader)
		return nil, err
	}

	client := lead.NewLeaderClient(conn)
	stream, err := client.JoinStandby(ctx)
	if err != nil {
		log.Fatalf("[FAIL] Unable to connect to stream STANDBY: %v", err)
		return nil, err
	}
	return stream, nil
}

func (d *LeaderNode) BeginHeartbeating(stream lead.Leader_JoinStandbyClient) {
	go func() {
		log.Print("[DEBUG] Standby heartbeating to Active")
		if d.heartbeat(stream) != nil {
			log.Printf("[WARNING] Unable to heartbeat to leader")
			return
		}
		for {
			if d.heartbeat(stream) != nil {
				log.Printf("[WARNING] Standby unable to heartbeat to leader")
				return
			}
			time.Sleep(time.Second)
		}
	}()
}

func (d *LeaderNode) heartbeat(stream lead.Leader_JoinStandbyClient) error {
	hb := lead.StandbyHb{
		Heartbeat:             true,
		MembershipListVersion: d.MembershipList.GetVersion(),
		LeaderListVersion:     d.LeaderList.GetVersion(),
		Ip:                    d.GetIp(),
	}
	//log.Print("[DEBUG] Standby Heartbeat")
	return stream.Send(&hb)
}

func (d *LeaderNode) ListenToLeader(stream lead.Leader_JoinStandbyClient, active string) {
	go func() {
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				log.Printf("[WARNING] Lost connection to leader: %v", err)
				return
			}
			if err != nil {
				log.Printf("[WARNING] Lost connection to leader: %v", err)
				d.LeaderList.Delete(active)
				d.MembershipList.Delete(active)
				for _, leader := range d.LeaderList.GetMembers() {
					//log.Printf("[DEBUG] This machine: %v", d.GetIp())
					//log.Printf("[DEBUG] Looking at %v for active leader", leader)
					if leader == d.GetIp() {
						log.Printf("[DEBUG] Setting this node as active leader")
						d.SetActive()
						return
					}
					stream, err := ConnectToLeaderStandby(context.Background(), leader+leader_port)
					if err != nil {
						log.Printf("[DEBUG] [WARNING] Failed to connect to new leader %v", leader)
						continue
					}
					log.Printf("[DEBUG] Connecting to new active leader %v", leader)
					d.BeginHeartbeating(stream)
					d.ListenToLeader(stream, leader)
					return
				}
				return
			} else {
				d.MembershipList = ml.CreateMembershipList(
					resp.GetMembers(),
					resp.GetMembershipListVersion(),
				)
				d.LeaderList = ml.CreateMembershipList(
					resp.GetLeaders(),
					resp.GetLeaderListVersion(),
				)
				log.Printf("[DEBUG] Received member list: version %v", d.MembershipList.GetVersion())

			}
		}
	}()
}
