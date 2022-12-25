package coordinator

import (
	"context"
	"errors"
	"log"
	"math"
	"math/rand"
	"net"
	"reflect"
	"time"

	coord "cs425-mp4/proto/coordinator"
	work "cs425-mp4/proto/worker"
	utils "cs425-mp4/utils"

	"google.golang.org/grpc"
)

const (
	max_failures = 3
	worker_port  = ":7000"
)

func NewCoordinator() Coordinator {
	this_address := utils.GetOutboundIP() + worker_port // vm test
	//this_address := utils.GetOutboundIP() + ":6999" // local test
	var model_list []Model
	machine := Coordinator{
		MemberList:        make(map[string]map[string]string),
		MemberListVersion: 0,
		FileList:          make(map[string]map[string]string),
		FileListVersion:   0,
		Standby:           "",
		Address:           this_address,
		ModelList:         model_list,
		JobList:           make(map[string]map[int][]int32),
		Todo:              make(map[int32]bool),
		InProgress:        make(map[int32]bool),
		Completed:         make(map[int32]bool),
	}
	machine.AddMember(this_address)
	return machine
}

func (c *Coordinator) AddMember(address string) {
	// Address is expected to contain the port
	if _, ok := c.MemberList[address]; !ok {
		c.MemberList[address] = make(map[string]string)
	}
	c.MemberListVersion++
}

func (c *Coordinator) DeleteMember(address string) {
	// Address is expected to contain the port
	// Deleting from FileList
	files := utils.GetKeys(c.MemberList[address])
	for _, file_name := range files {
		delete(c.FileList[file_name], address)
	}
	delete(c.MemberList, address) // Needs to be done last so that the filelist of the member can still be accessed
	c.MemberListVersion++
	c.FileListVersion++
}

func (c *Coordinator) AddFile(address string, file_name string) {
	// Checks if file is in the file list
	if _, ok := c.FileList[file_name]; ok {
		// If the file is present in the file list, add the address storing the file to the "set"
		c.FileList[file_name][address] = "1"
	} else {
		// Otherwise, create a new "set" in the file list
		c.FileList[file_name] = make(map[string]string)
		c.FileList[file_name][address] = "1"
	}
	// Also add it to the member list
	c.MemberList[address][file_name] = "1"
	c.MemberListVersion++
	c.FileListVersion++
}

func (c *Coordinator) DeleteFile(address string, file_name string) {
	delete(c.FileList, file_name)
	delete(c.MemberList[address], file_name)
	c.MemberListVersion++
	c.FileListVersion++
}

func (c *Coordinator) GetAddress() string {
	return c.Address
}

func (c *Coordinator) StartServer(port string) {
	go func() {
		lis, err := net.Listen("tcp", port)
		if err != nil {
			log.Fatalf("[FAIL] Unable to listen on port: %v\n%v", port, err)
		}

		server := grpc.NewServer()
		coord.RegisterCoordinatorServer(server, c)
		log.Printf("[DEBUG] Coordinator server now serving")
		if err := server.Serve(lis); err != nil {
			log.Fatalf("[FAIL] Unable to serve\n%v", err)
		}
	}()
}

func (c *Coordinator) GetAvailableWorkers() [max_failures + 1]string {
	rand.Seed(time.Now().Unix())
	num_replicas := int(math.Min(float64(len(c.MemberList)), float64(max_failures+1)))
	workers := utils.GetMembers(c.MemberList)
	chosen_replicas := [max_failures + 1]string{}
	permutation := rand.Perm(len(c.MemberList))
	for i := 0; i < num_replicas; i++ {
		chosen_replicas[i] = workers[permutation[i]]
	}
	return chosen_replicas
}

func (c *Coordinator) PutHandler(address string, file_name string, file []byte) error {
	// Address is expected to contain the port
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial %v: %v", address, err)
		return err
	}
	client := work.NewWorkerClient(conn)
	send_file := work.File{FileName: file_name, File: file}
	_, err = client.Put(context.Background(), &send_file)
	if err != nil {
		log.Printf("[FAIL] Unable to put file %v at %v: %v", file_name, address, err)
		return err
	}
	//log.Printf("[SUCCESS] Put file %v at %v", file_name, address)
	c.AddFile(address, file_name)
	return nil
}

func (c *Coordinator) DeleteHandler(address string, file_name string) error {
	// Address is expected to contain the port
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial %v: %v", address, err)
		return err
	}
	client := work.NewWorkerClient(conn)
	send_msg := work.File{FileName: file_name}
	_, err = client.Delete(context.Background(), &send_msg)
	if err != nil {
		log.Printf("[FAIL] Unable to delete file %v from %v: %v", file_name, address, err)
		return err
	}
	//log.Printf("[SUCCESS] Delete file %v", file_name)
	c.DeleteFile(address, file_name)
	return nil
}

func GetHandler(address string, file_name string) ([]byte, error) {
	// Address is expected to contain the port
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial %v: %v", address, err)
		return nil, err
	}
	client := work.NewWorkerClient(conn)
	send_msg := work.File{FileName: file_name}
	recv, err := client.Get(context.Background(), &send_msg)
	if err != nil {
		log.Printf("[FAIL] Unable to get file %v from %v: %v", file_name, address, err)
		return nil, err
	}
	file := recv.GetFile()
	log.Printf("[SUCCESS] Get operation of file %v", file_name)
	return file, nil
}

func GetVersionHandler(address string, file_name string, versions int32) ([]byte, error) {
	// Address is expected to contain the port
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial %v: %v", address, err)
		return nil, err
	}
	client := work.NewWorkerClient(conn)
	send_msg := work.FileVersions{FileName: file_name, Versions: versions}
	recv, err := client.GetVersion(context.Background(), &send_msg)
	if err != nil {
		log.Printf("[FAIL] Unable to get file %v from %v: %v", file_name, address, err)
		return nil, err
	}
	file := recv.GetFile()
	log.Printf("[SUCCESS] Get operation of file %v with %v versions", file_name, versions)
	return file, nil
}

func ElectStandbyHandler(address string) error {
	// Address is expected to contain the port
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial %v: %v", address, err)
		return err
	}
	client := work.NewWorkerClient(conn)
	empty := work.Empty{}
	_, err = client.ElectStandby(context.Background(), &empty)
	if err != nil {
		log.Printf("[FAIL] Unable to elect %v as standby: %v", address, err)
	}
	return nil
}

func (c *Coordinator) SendHandler(list_type string) error {
	// Standby address is based on worker ip, so worker port must be stripped off
	address := utils.StripPort(c.Standby) + coordinator_port
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial %v: %v", address, err)
		return err
	}
	client := coord.NewCoordinatorClient(conn)
	var keys []string
	var lst []*coord.List
	var version int32
	if list_type == "Member" {
		version = int32(c.MemberListVersion)
		keys = utils.GetMembers(c.MemberList)
		lst = make([]*coord.List, len(keys))
		for i, key := range keys {
			lst[i] = &coord.List{Vals: utils.GetKeys(c.MemberList[key])}
		}
	} else if list_type == "File" {
		version = int32(c.FileListVersion)
		keys = utils.GetMembers(c.FileList)
		lst = make([]*coord.List, len(keys))
		for i, key := range keys {
			lst[i] = &coord.List{Vals: utils.GetKeys(c.FileList[key])}
		}
	} else {
		log.Printf("[FAIL] Invalid list type")
		return errors.New("invalid list type")
	}
	new_list_msg := coord.NetworkInfo{Type: list_type, Keys: keys, Lsts: lst, Version: version}
	_, err = client.UpdateNetworkInfo(context.Background(), &new_list_msg)
	if err != nil {
		log.Printf("[FAIL] Unable to update %vList at standby: %v", list_type, err)
		return err
	}
	//log.Printf("[SUCCESS] Sent %vList to standby", list_type)
	return nil
}

func (c *Coordinator) SetActive() {
	c.UpdateActive()
	c.StandbyHealthCheck()
}

func (c *Coordinator) UpdateActive() error {
	members := utils.GetMembers(c.MemberList)
	for _, address := range members {
		if err := UpdateActiveHandler(address, utils.GetOutboundIP()+coordinator_port); err != nil {
			return err
		}
	}
	log.Print("[SUCCESS] Update active coordinator in network")
	return nil
}

func UpdateActiveHandler(member string, active string) error {
	// Member is expected to contain the port
	conn, err := grpc.Dial(member, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial %v: %v", member, err)
		return err
	}
	client := work.NewWorkerClient(conn)
	new_coord_msg := work.Coordinator{Ip: active}
	_, err = client.UpdateActive(context.Background(), &new_coord_msg)
	if err != nil {
		log.Printf("[FAIL] Unable to update active coordinator at %v: %v", member, err)
		return err
	}
	return nil
}

func (c *Coordinator) AddModel(batch_size int32) {
	id := len(c.ModelList)
	new_model := NewModel(id, batch_size)
	new_model.StartClock()
	c.ModelList = append(c.ModelList, new_model)
}

func (c *Coordinator) TrainHandler(member string) error {
	// Member is expected to contain the port
	conn, err := grpc.Dial(member, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial %v: %v", member, err)
		return err
	}
	client := work.NewWorkerClient(conn)
	id := len(c.ModelList) - 1
	job_msg := work.Job{JobId: int32(id)}
	_, err = client.Train(context.Background(), &job_msg)
	if err != nil {
		log.Printf("[DEBUG] Unable to train model %v at %v: %v", id, member, err)
		return err
	}
	//log.Printf("[SUCCESS] Trained model %v at %v", c.ModelCounter, member)
	return nil
}

func (c *Coordinator) CreateJobs(n int) {
	for i := 0; i < n; i++ {
		c.Todo[int32(i)] = true
	}
}

func (c *Coordinator) GetJobs() []int32 {
	keys := reflect.ValueOf(c.Todo).MapKeys()
	jobs := make([]int32, len(keys))
	for i := 0; i < len(keys); i++ {
		jobs[i] = int32(keys[i].Int())
	}
	return jobs
}

func (c *Coordinator) SelectNJobs(n int) []int32 {
	rand.Seed(time.Now().Unix())
	var selected []int32
	permutation := rand.Perm(len(c.Todo))
	jobs := c.GetJobs()
	num_jobs := math.Min(float64(n), float64(len(c.Todo)))
	for i := 0; i < int(num_jobs); i++ {
		selected = append(selected, jobs[permutation[i]])
	}

	// Remove from Todo into InProgress
	for _, job_id := range selected {
		delete(c.Todo, job_id)
		c.InProgress[job_id] = true
	}

	return selected
}

/*
func (c *Coordinator) InferenceHandler(member string, jobs []int32, model int) ([]byte, error) {
	// Member is expected to contain the port
	conn, err := grpc.Dial(member, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial %v: %v", member, err)
		return nil, err
	}
	client := work.NewWorkerClient(conn)
	query := work.Query{JobId: jobs}
	c.JobList[member] = make(map[int][]int32)
	c.JobList[member][model] = jobs
	start := time.Now()
	resp, err := client.Inference(context.Background(), &query)
	if err != nil {
		log.Printf("[FAIL] %v failed to inference: %v", member, err)
		log.Printf("[DEBUG] Resetting jobs: %v", jobs)
		c.ResetJobs(jobs)
		return nil, err
	}
	// inference success
	duration := float64(time.Since(start)/time.Millisecond) / float64(len(jobs))
	model_id := int32(model)
	c.ModelList[model_id].AddJobs(len(jobs))
	c.ModelList[model_id].AddTime(duration)

	results := resp.GetResults()
	for _, job_id := range jobs {
		delete(c.InProgress, job_id)
		c.Completed[job_id] = true
	}
	delete(c.JobList[member], model)
	log.Printf("[SUCCESS] Inference batch at %v", member)
	return results, nil
}
*/

func (c *Coordinator) ResetJobs(jobs []int32) {
	for _, job := range jobs {
		delete(c.InProgress, job)
		c.Todo[job] = true
	}
}

func GetInferenceStream(ctx context.Context, ip string) (work.Worker_InferenceClient, error) {
	conn, err := grpc.Dial(ip, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial coordinator: %v\n%v", ip, err)
		return nil, err
	}

	client := work.NewWorkerClient(conn)
	stream, err := client.Inference(ctx)
	if err != nil {
		log.Printf("[FAIL] Unable to fetch stream from %v: %v", ip, err)
		return nil, err
	}
	return stream, nil
}

func (c *Coordinator) GetAvailableInference() (string, int, error, int) {
	members := utils.GetMembers(c.MemberList)
	rand.Shuffle(len(members), func(i, j int) {
		members[i], members[j] = members[j], members[i]
	})
	//fmt.Printf("[DEBUG] Top of MemberList: %v\n", members[0])

	var models []Model
	first := c.ModelList[0]
	second := c.ModelList[1]
	// fmt.Printf("[DEBUG] Model %v processed: %v\n", first.GetId(), first.GetProcessed())
	// fmt.Printf("[DEBUG] Model %v processed: %v\n", second.GetId(), second.GetProcessed())

	var diff float64
	if first.GetProcessed() > second.GetProcessed() {
		models = append(models, second, first)
		diff = (float64(first.GetProcessed())*1.05 - float64(second.GetProcessed())) + 1
	} else {
		models = append(models, first, second)
		diff = (float64(second.GetProcessed())*1.05 - float64(first.GetProcessed())) + 1
	}

	for _, model := range models {
		model_id := model.GetId()
		for _, member := range members {
			if c.Scheduler[member][model_id] {
				batch_size := model.GetBatchSize()
				n_jobs := int(math.Min(diff, float64(batch_size)))
				return member, model_id, nil, n_jobs
			}
		}
	}
	return "", -1, errors.New("no models available"), -1
}
