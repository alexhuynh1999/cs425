package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	coord "cs425-mp4/proto/coordinator"
	"cs425-mp4/proto/worker"
	work "cs425-mp4/proto/worker"
	utils "cs425-mp4/utils"

	"google.golang.org/grpc"
)

type Coordinator struct {
	coord.UnimplementedCoordinatorServer
	sync.Mutex
	// Syntax: map[ip][list_of_files]
	// *Chose map vs list because maps will get us an accurate list of who's in the network + fast inserts/deletes. Ignore the last string value - this is a nuance because Go does not have hashsets
	MemberList        map[string]map[string]string
	MemberListVersion int
	// Syntax: map[file_name][list_of_ips]*
	FileList        map[string]map[string]string
	FileListVersion int
	Standby         string
	Address         string
	// Syntax: map[model_number]batch_size
	ModelList []Model
	// Syntax: map[ip][model_number][job_ids];
	JobList map[string]map[int][]int32
	// Syntax: map[ip][model_number]stream
	InferenceStreams map[string]map[int]worker.Worker_InferenceClient
	// Syntax: map[ip][model_number]status; true = alive, false = busy
	Scheduler map[string]map[int]bool
	// Syntax: map[job_id]
	Todo              map[int32]bool
	InProgress        map[int32]bool
	Completed         map[int32]bool
	Status            string
	CurrentJobQueries int32
	Client            string
	Results           []byte
}

var (
	done = coord.Ack{Done: true}
)

// THESE ARE THE FUNCTIONS FOR THE CLIENT-FACING SERVICES

func (coordinator *Coordinator) ListFiles(ctx context.Context, in *coord.File) (*coord.FileLocations, error) {
	file_name := in.GetFileName()
	// Checks if file is in file list
	if file_locations, ok := coordinator.FileList[file_name]; ok {
		ips := utils.GetKeys(file_locations)
		vms := utils.MapIpsToVms(ips)
		msg := coord.FileLocations{
			Ips: ips,
			Vms: vms,
		}
		return &msg, nil
	}
	return nil, errors.New("file not found")
}

func (coordinator *Coordinator) Store(ctx context.Context, in *coord.Heartbeat) (*coord.FileList, error) {
	address := in.GetIp()
	if files, ok := coordinator.MemberList[address]; ok {
		files := utils.GetKeys(files)
		msg := coord.FileList{Files: files}
		return &msg, nil
	} else {
		error_msg := fmt.Sprintf("%v not in network", address)
		return nil, errors.New(error_msg)
	}
}

func (coordinator *Coordinator) GetMembers(ctx context.Context, in *coord.Empty) (*coord.MemberList, error) {
	ips := utils.GetMembers(coordinator.MemberList)
	vms := utils.MapIpsToVms(ips)
	msg := coord.MemberList{
		Ips: ips,
		Vms: vms,
	}
	return &msg, nil
}

func (coordinator *Coordinator) Put(ctx context.Context, in *coord.File) (*coord.Ack, error) {
	file_name := in.GetFileName()
	file := in.GetFile()
	var workers [4]string
	// Checks if file currently exists in network
	if file_locations, ok := coordinator.FileList[file_name]; ok {
		// Retrieves the VMs where the file is currently stored
		copy(workers[:], utils.GetKeys(file_locations))
	} else {
		// Else statement implies file does not exist in network
		// Randomly chooses workers to copy file into
		workers = coordinator.GetAvailableWorkers() // Helper
	}

	for _, ip := range workers {
		// Because of how slices work in Go, if we have < 4 machines in the network, we'll just have empty elements in the array.
		// An empty element implies there are no more workers
		if ip == "" {
			break
		}
		// PutHandler is a helper fuction
		if err := coordinator.PutHandler(ip, file_name, file); err != nil {
			log.Print("[FAIL] PutHandler failed")
		}
	}
	log.Printf("[SUCCESS] Put file %v into the network", file_name)
	return &done, nil
}

func (coordinator *Coordinator) Get(ctx context.Context, in *coord.File) (*coord.File, error) {
	file_name := in.GetFileName()
	// Checks if file is in FileList
	if file_locations, ok := coordinator.FileList[file_name]; ok {
		// Retrieves a random machine that has the file
		address := utils.GetKeys(file_locations)[0]
		file, err := GetHandler(address, file_name) // Helper
		if err != nil {
			return nil, err
		}
		// Returns the file
		send_msg := coord.File{File: file}
		return &send_msg, nil
	} else {
		// If file not in FileList, return not found
		return nil, errors.New("file not found")
	}
}

func (coordinator *Coordinator) Delete(ctx context.Context, in *coord.File) (*coord.Ack, error) {
	file_name := in.GetFileName()
	// Checks if file is in FileList
	if file_locations, ok := coordinator.FileList[file_name]; ok {
		// Gets every location of the file as a list
		file_locations_lst := utils.GetKeys(file_locations)
		// Loops through the list and deletes each file individually
		for _, address := range file_locations_lst {
			if err := coordinator.DeleteHandler(address, file_name); err != nil {
				log.Print("[FAIL] DeleteHandler failed")
			}
		}
		log.Printf("[SUCCESS] Delete file %v", file_name)
		return &done, nil
	} else {
		// If file not in FileList, return not found
		return nil, errors.New("file not found")
	}
}

func (coordinator *Coordinator) GetVersion(ctx context.Context, in *coord.FileVersions) (*coord.File, error) {
	file_name := in.GetFileName()
	versions := in.GetVersions()
	// Checks if file is in FileList
	if file_locations, ok := coordinator.FileList[file_name]; ok {
		// Retrieves a random machine that has the file
		address := utils.GetKeys(file_locations)[0]
		file, err := GetVersionHandler(address, file_name, versions) // Helper
		if err != nil {
			return nil, err
		}
		// Returns the file
		send_msg := coord.File{File: file}
		return &send_msg, nil
	} else {
		// If file not in FileList, return not found
		return nil, errors.New("file not found")
	}
}

func (coordinator *Coordinator) Join(server coord.Coordinator_JoinServer) error {
	req, err := server.Recv()
	if err != nil {
		log.Printf("[ERROR] Machine failed to join network: %v", err)
	}

	incoming_ip := req.GetIp()
	ctx := server.Context()

	// STANDBY HANDLER
	// NOTE: when doing standby activities, the worker port must be stripped off of it
	if strings.Contains(incoming_ip, "STANDBY") {
		// Send both the members and files to standby
		if err := coordinator.SendHandler("Member"); err != nil {
			log.Printf("[FAIL] Unable to update members at standby %v: %v", coordinator.Standby, err)
		}
		if err := coordinator.SendHandler("File"); err != nil {
			log.Printf("[FAIL] Unable to update files at standby %v: %v", coordinator.Standby, err)
		}

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			hb, err := server.Recv()
			if err != nil {
				// If there is an error, that means the machine has disconnected.
				log.Printf("[DEBUG] Standby has left the network: %v", err)
				coordinator.Standby = ""
			}
			standby_mListVersion := hb.GetMemberListVersion()
			standby_fListVersion := hb.GetFileListVersion()
			if standby_mListVersion != int32(coordinator.MemberListVersion) {
				if err := coordinator.SendHandler("Member"); err != nil {
					log.Printf("[FAIL] Unable to update members at standby %v: %v", coordinator.Standby, err)
				}
			}
			if standby_fListVersion != int32(coordinator.FileListVersion) {
				if err := coordinator.SendHandler("File"); err != nil {
					log.Printf("[FAIL] Unable to update files at standby %v: %v", coordinator.Standby, err)
				}
			}
		}
	}

	// MEMBER HANDLER
	log.Printf("[DEBUG] Adding %v to the network", incoming_ip)
	coordinator.AddMember(incoming_ip) // Helper
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, err := server.Recv()
		if err != nil {
			// If there is an error, that means the machine has disconnected.
			log.Printf("[DEBUG] Removing %v from the network", incoming_ip)
			coordinator.DeleteMember(incoming_ip) // Helper
			if model_jobs, ok := coordinator.JobList[incoming_ip]; ok {
				for model_id, _ := range coordinator.ModelList {
					jobs := model_jobs[model_id]
					for _, job := range jobs {
						coordinator.Todo[job] = true
					}
				}
			}
			delete(coordinator.JobList, incoming_ip)

		}
	}
}

// FAULT-TOLERANCE CORE FUNCTIONS - CALLED IN helper/StartServer
// TODO:
//	[] replicas
//	[x] elect standby
//		[x] standby health check
//		[x] standby heartbeat to leader

func (coordinator *Coordinator) StandbyHealthCheck() {
	go func() {
		for {
			// Checks if the current standby is alive
			if _, ok := coordinator.MemberList[coordinator.Standby]; ok {
				time.Sleep(time.Second)
				continue
			}
			// If not, generate a random new member to be the standby
			log.Print("[DEBUG] Searching for new standby\n")
			members := utils.GetMembers(coordinator.MemberList)
			new_standby := utils.GetRandomMember(members)
			// The new standby can not be the coordinator itself. Continuously generate until a unique one is generated.
			for strings.Contains(new_standby, utils.GetOutboundIP()) {
				members = utils.GetMembers(coordinator.MemberList)
				new_standby = utils.GetRandomMember(members)
				time.Sleep(time.Second)
			}
			// Set the standby
			log.Printf("[DEBUG] New standby found. Setting %v as standby\n", new_standby)
			coordinator.Standby = new_standby
			if err := ElectStandbyHandler(new_standby); err != nil {
				log.Printf("[FAIL] Unable to start standby coordinator server at %v: %v", new_standby, err)
			}
			address := utils.StripPort(new_standby) + coordinator_port
			conn, err := grpc.Dial(address, grpc.WithInsecure())
			if err != nil {
				log.Printf("[FAIL] Unable to dial %v: %v", address, err)
				return
			}
			client := coord.NewCoordinatorClient(conn)
			for _, model := range coordinator.ModelList {
				msg := coord.ModelInfo{
					Id:        int32(model.GetId()),
					BatchSize: model.GetBatchSize(),
				}
				_, err := client.CreateModel(context.Background(), &msg)
				if err != nil {
					log.Printf("[FAIL] Unable to create model at standby %v", err)
				}
				var times []float32
				for _, time := range model.process_times {
					times = append(times, float32(time))
				}
				update := coord.ModelStats{
					ModelId: int32(model.GetId()),
					NumJobs: model.GetProcessed(),
					//Time:    times,
					UpdateType: "jobs",
				}
				_, err = client.UpdateModel(context.Background(), &update)
				if err != nil {
					log.Printf("[FAIL] Unable to update model at standby: %v", err)
				}
				update = coord.ModelStats{
					ModelId: int32(model.GetId()),
					//NumJobs: model.GetProcessed(),
					Time:       times,
					UpdateType: "times",
				}
				_, err = client.UpdateModel(context.Background(), &update)
				if err != nil {
					log.Printf("[FAIL] Unable to update model at standby: %v", err)
				}
			}
			time.Sleep(time.Millisecond * 100)
		}
	}()
}

// MP4 Services
func (coordinator *Coordinator) Train(ctx context.Context, in *coord.Hyperparameters) (*coord.Ack, error) {
	batch_size := in.GetBatchSize()
	coordinator.AddModel(batch_size)
	members := utils.GetMembers(coordinator.MemberList)

	// SEND TO STANDBY
	address := utils.StripPort(coordinator.Standby) + coordinator_port
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial %v: %v", address, err)
		return nil, err
	}
	client := coord.NewCoordinatorClient(conn)
	model_id := len(coordinator.ModelList) - 1
	model_msg := coord.ModelInfo{Id: int32(model_id), BatchSize: batch_size}
	_, err = client.CreateModel(context.Background(), &model_msg)
	if err != nil {
		log.Printf("[FAIL] Unable to send model to standby: %v", err)
		return nil, err
	}

	for _, member := range members {
		if err := coordinator.TrainHandler(member); err != nil {
			log.Printf("[FAIL] TrainHandler failed on %v", member)
			return nil, err
		}
	}
	log.Printf("[SUCCESS] Trained model %v in network", len(coordinator.ModelList)-1)
	return &done, nil
}

// TODO: Inference
// Assumptions:
//	- Each node already has the sharded dataset
// Algorithm:
//	- Coordinator gets job
//	- Assigns VM1M1 batchsize 1 queries
//	- Assigns VM1M2 batchsize 2 queries
//	- Assigns VM2M1 batchsize 1 queries
//	...
//	- Fin

// Fault-tolerant:
//   - Reassign the batchsize 1 queries
//   - If coordinator dies, standby will just restart whole job

/*
func (coordinator *Coordinator) Inference(ctx context.Context, in *coord.Query) (*coord.QueryResults, error) {
	num_queries := int(in.GetNumQueries())
	var results []byte
	coordinator.Completed = make(map[int32]bool)
	coordinator.CreateJobs(num_queries)
	for len(coordinator.Completed) < num_queries {
		for _, model := range coordinator.ModelList {
			batch_size := model.GetBatchSize()
			members := utils.GetMembers(coordinator.MemberList)
			for _, member := range members {
				batch_jobs := coordinator.SelectNJobs(int(batch_size))
				batch_result, err := coordinator.InferenceHandler(member, batch_jobs, model.GetId())
				if err != nil {
					log.Printf("[FAIL] InferenceHandler failed on %v", member)
					break
				}
				results = append(results, batch_result...)
				model.AddJobs(len(batch_jobs))
			}
		}
	}
	query_results := coord.QueryResults{Results: results}
	log.Printf("[SUCCESS] Inference")
	return &query_results, nil
}
*/
/*
func (coordinator *Coordinator) Inference(ctx context.Context, in *coord.Query) (*coord.QueryResults, error) {
	coordinator.InferenceStreams = make(map[string]map[int]work.Worker_InferenceClient)
	coordinator.Scheduler = make(map[string]map[int]bool)
	members := utils.GetMembers(coordinator.MemberList)
	for _, member := range members {
		coordinator.InferenceStreams[member] = make(map[int]work.Worker_InferenceClient)
		coordinator.Scheduler[member] = make(map[int]bool)
		for _, model := range coordinator.ModelList {
			model_id := model.GetId()
			stream, err := GetInferenceStream(context.Background(), member)
			if err != nil {
				log.Printf("[FAIL] Unable to get inference stream at %v for model %v: %v", member, model_id, err)
				return nil, err
			}
			coordinator.InferenceStreams[member][model_id] = stream
			coordinator.Scheduler[member][model_id] = true
		}
	}
	log.Printf("[DEBUG] Connected to all worker/model streams in network")
	log.Printf("[DEBUG] All streams set to available")

	num_queries := in.GetNumQueries()
	coordinator.CreateJobs(int(num_queries))
	var results []byte
	for len(coordinator.Completed) < int(num_queries) {
		member, model_id, err := coordinator.GetAvailableInference()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		// Address/Model is available to inference
		stream := coordinator.InferenceStreams[member][model_id]
		model := coordinator.ModelList[model_id]
		batch_size := model.GetBatchSize()
		jobs := coordinator.SelectNJobs(int(batch_size))
		go func() {
			query := work.Query{JobId: jobs, ModelId: int32(model_id)}
			stream.Send(&query)

			for {
				fmt.Printf("[DEBUG] Awaiting response...\n")
				resp, err := stream.Recv()
				if err != nil {
					log.Printf("[DEBUG] Failed to receive from %v: %v", member, err)
					return
				}
				query_result := resp.GetResults()
				times := resp.GetTimes()
				results = append(results, query_result...)
				for _, time := range times {
					model.AddTime(float64(time))
				}
				model.AddJobs(int(batch_size))
			}
		}()

		coordinator.Scheduler[member][model_id] = false
	}
	out := coord.QueryResults{Results: results}
	return &out, nil
}
*/

func (coordinator *Coordinator) Inference(server coord.Coordinator_InferenceServer) error {
	query, err := server.Recv()
	if err != nil {
		log.Printf("")
	}
	num_queries := query.GetNumQueries()
	client_address := query.GetIp()
	coordinator.InferenceStreams = make(map[string]map[int]work.Worker_InferenceClient)
	coordinator.Scheduler = make(map[string]map[int]bool)
	members := utils.GetMembers(coordinator.MemberList)
	for _, member := range members {
		coordinator.InferenceStreams[member] = make(map[int]work.Worker_InferenceClient)
		coordinator.Scheduler[member] = make(map[int]bool)
		for _, model := range coordinator.ModelList {
			model_id := model.GetId()
			stream, err := GetInferenceStream(context.Background(), member)
			if err != nil {
				log.Printf("[FAIL] Unable to get inference stream at %v for model %v: %v", member, model_id, err)
				return err
			}
			coordinator.InferenceStreams[member][model_id] = stream
			coordinator.Scheduler[member][model_id] = true
		}
	}
	log.Printf("[DEBUG] Connected to all worker/model streams in network")
	log.Printf("[DEBUG] All streams set to available")
	//server.Send(&coord.QueryResults{Results: []byte("Hello World")})

	coordinator.Completed = make(map[int32]bool)
	coordinator.CreateJobs(int(num_queries))
	completed := 0
	coordinator.Status = "working"
	coordinator.Client = client_address
	coordinator.Results = []byte("")
	// SEND TO STANDBY
	address := utils.StripPort(coordinator.Standby) + coordinator_port
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial %v: %v", address, err)
		return nil
	}
	client := coord.NewCoordinatorClient(conn)
	model_msg := coord.Status{
		Working:    true,
		NumQueries: num_queries,
		Client:     client_address,
	}
	_, err = client.UpdateJobStatus(context.Background(), &model_msg)
	if err != nil {
		log.Printf("[FAIL] Unable to update standby status: %v", err)
		return nil
	}
	for completed < int(num_queries) {
		member, model_id, err, n_jobs := coordinator.GetAvailableInference()
		if err != nil {
			//fmt.Printf("[DEBUG] All models busy. Waiting\n")
			time.Sleep(time.Second)
			continue
		}
		// Address/Model is available to inference
		stream := coordinator.InferenceStreams[member][model_id]
		model := &coordinator.ModelList[model_id]
		//batch_size := model.GetBatchSize()
		jobs := coordinator.SelectNJobs(n_jobs)
		coordinator.JobList[member] = make(map[int][]int32)
		coordinator.JobList[member][model_id] = jobs
		coordinator.Scheduler[member][model_id] = false
		query := work.Query{JobId: jobs, ModelId: int32(model_id)}
		model.AddJobs(n_jobs)
		// SEND TO STANDBY
		for coordinator.Standby == "" && len(utils.GetMembers(coordinator.MemberList)) > 1 {
			// stall for standby if available
			time.Sleep(time.Second)
		}
		address := utils.StripPort(coordinator.Standby) + coordinator_port
		conn, err := grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			log.Printf("[FAIL] Unable to dial %v: %v", address, err)
			return nil
		}
		client := coord.NewCoordinatorClient(conn)
		model_msg := coord.ModelStats{
			ModelId:    int32(model_id),
			NumJobs:    int32(len(jobs)),
			UpdateType: "jobs",
		}
		_, err = client.UpdateModel(context.Background(), &model_msg)
		if err != nil {
			log.Printf("[FAIL] Unable to update jobs at standby: %v", err)
			return nil
		}
		stream.Send(&query)
		completed += len(jobs)

		go func(jobs []int32) {
			for {
				resp, err := stream.Recv()
				if err != nil {
					//log.Printf("[DEBUG] Failed to receive from %v: %v", member, err)
					coordinator.ResetJobs(jobs)
					completed -= len(jobs)
					return
				}
				query_result := resp.GetResults()
				if len(query_result) == 0 {
					continue
				}
				coordinator.Results = append(coordinator.Results, query_result...)
				//fmt.Printf("[DEBUG] Add jobs: %v\n", jobs)
				//fmt.Printf("[DEBUG] Query result: \n%v from %v model %v\n", string(query_result), member, model_id)
				times := resp.GetTimes()
				for _, time := range times {
					model.AddTime(float64(time))
				}
				for coordinator.Standby == "" && len(utils.GetMembers(coordinator.MemberList)) > 1 {
					address := utils.StripPort(coordinator.Standby) + coordinator_port
					conn, err := grpc.Dial(address, grpc.WithInsecure())
					if err != nil {
						log.Printf("[FAIL] Unable to dial %v: %v", address, err)
						return
					}
					client := coord.NewCoordinatorClient(conn)
					model_msg := coord.ModelStats{
						ModelId:    int32(model_id),
						Time:       times,
						UpdateType: "times",
					}
					_, err = client.UpdateModel(context.Background(), &model_msg)
					if err != nil {
						log.Printf("[FAIL] Unable to update times at standby: %v", err)
						return
					}
				}

				for _, job_id := range jobs {
					delete(coordinator.InProgress, job_id)
					coordinator.Completed[job_id] = true
				}
				coordinator.Scheduler[member][model_id] = true
				query_results := coord.QueryResults{Results: query_result}
				delete(coordinator.JobList[member], model_id)

				server.Send(&query_results)
			}
		}(jobs)
		time.Sleep(time.Second)
	}
	address = utils.StripPort(coordinator.Standby) + coordinator_port
	conn, err = grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial %v: %v", address, err)
		return nil
	}
	client = coord.NewCoordinatorClient(conn)
	model_msg = coord.Status{
		Working: false,
	}
	_, err = client.UpdateJobStatus(context.Background(), &model_msg)
	if err != nil {
		log.Printf("[FAIL] Unable to update standby status: %v", err)
		return nil
	}
	log.Printf("[SUCCESS] Inference on %v queries", num_queries)
	coordinator.Status = "ready"
	coordinator.Client = ""
	return nil
}

func (coordinator *Coordinator) GetRate(ctx context.Context, in *coord.Model) (*coord.C1, error) {
	if len(coordinator.ModelList) == 0 {
		log.Printf("[WARNING] No models have been trained")
		return nil, errors.New("no models in network")
	}

	model_id := in.GetModelId()
	if int(model_id) >= len(coordinator.ModelList) {
		log.Printf("[WARNING] %v is not in the ModelList", model_id)
		return nil, errors.New("model not found")
	}
	model := coordinator.ModelList[model_id]
	rate := model.GetRate(10)
	jobs_processed := model.GetProcessed()
	out := coord.C1{Rate: rate, Processed: jobs_processed}
	return &out, nil
}

func (coordinator *Coordinator) Stats(ctx context.Context, in *coord.Model) (*coord.C2, error) {
	if len(coordinator.ModelList) == 0 {
		log.Printf("[WARNING] No models have been trained")
		return nil, errors.New("no models in network")
	}

	model_id := in.GetModelId()
	if int(model_id) > len(coordinator.ModelList) {
		log.Printf("[WARNING] %v is not in the ModelList", model_id)
		return nil, errors.New("model not found")
	}
	model := coordinator.ModelList[model_id]
	if len(model.process_times) == 0 {
		return nil, errors.New("model has not processed any jobs")
	}

	mean, std, median, percentiles := model.GetStats()
	out := coord.C2{
		Mean:        mean,
		Std:         std,
		Median:      float32(median),
		Percentiles: percentiles,
	}
	return &out, nil
}

func (coordinator *Coordinator) SetBatchSize(ctx context.Context, in *coord.Model) (*coord.Ack, error) {
	model_id := in.GetModelId()
	batch_size := in.GetBatchSize()
	if len(coordinator.ModelList) == 0 {
		log.Printf("[WARNING] No models have been trained")
		return nil, errors.New("no models in network")
	}

	if int(model_id) > len(coordinator.ModelList) {
		log.Printf("[WARNING] %v is not in the ModelList", model_id)
		return nil, errors.New("model not found")
	}
	model := coordinator.ModelList[model_id]
	model.SetBatchSize(batch_size)
	return &done, nil
}

func (coordinator *Coordinator) ShowJobs(ctx context.Context, in *coord.Empty) (*coord.C4, error) {
	var addresses []string
	var jobs []*coord.Jobs
	members := utils.GetMembers(coordinator.MemberList)
	for _, member := range members {
		for _, model := range coordinator.ModelList {
			ip := utils.StripPort(member)
			address := fmt.Sprintf("%v:MODEL%v", ip, model.GetId())
			addresses = append(addresses, address)

			job_ids := coordinator.JobList[member][model.GetId()]
			job_id_msg := coord.Jobs{JobIds: job_ids}
			jobs = append(jobs, &job_id_msg)
		}
	}
	out := coord.C4{
		Ips:  addresses,
		Jobs: jobs,
	}
	return &out, nil
}

func (coordinator *Coordinator) GetResults(ctx context.Context, in *coord.Empty) (*coord.Results, error) {
	result := string(coordinator.Results)
	out := coord.Results{Result: result}
	return &out, nil
}
