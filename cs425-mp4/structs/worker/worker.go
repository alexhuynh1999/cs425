package worker

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	coord "cs425-mp4/proto/coordinator"
	work "cs425-mp4/proto/worker"
	c "cs425-mp4/structs/coordinator"
	utils "cs425-mp4/utils"

	"google.golang.org/grpc"
)

type Worker struct {
	work.UnimplementedWorkerServer
	ip             string
	coordinator_ip string
}

var (
	done       = work.Ack{Done: true}
	lock_timer = 5
	delimiter  = "@@@"
)

func (w *Worker) Put(ctx context.Context, in *work.File) (*work.Ack, error) {
	file_name := in.GetFileName()
	file := in.GetFile()

	// Opens a file. If it doesn't exist, create a new one. If it does exist, writes will be appended.
	f, err := os.OpenFile(file_name, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("[ERROR] Failed to write file %v: %v", file_name, err)
	}
	// Reads from the file (if it didn't exist, this will just be a blank file)
	curr_file, err := os.ReadFile(file_name)
	if err != nil {
		log.Fatalf("[ERROR] Unable to read file %v: %v", file_name, err)
	}
	file_str := string(curr_file)

	// Gets the current version of the file by reading from the latest delimiter
	// File version will be stored as [DELIMITER][VERSION]
	// Example: "@@@4" -> version 4
	version_idx := strings.Index(file_str, delimiter)
	var version int
	if version_idx == -1 {
		// If there was no delimiter, that implies this file is empty / hasn't been written to. This will be the first version of the write.
		version = 0
	} else {
		// Otherwise, the version is just after the delimter
		// Note: when you index a string in Go, it reads the unicode number. Therefore, we need to convert from unicode -> str -> int to get the version
		version, _ = strconv.Atoi(string(file_str[version_idx+len(delimiter)]))
	}
	version++

	// Creating the byte arrays of the header + new file + old file
	header := fmt.Sprintf("%v%v\n", delimiter, version)
	header_bytes := []byte(header)
	new_line := []byte("\n")
	old_file := []byte(file_str)
	// Writing in this order so the newest version is on top
	f.Write(header_bytes)
	f.Write(file)
	f.Write(new_line)
	f.Write(old_file)
	f.Close()

	return &done, nil
}

func (w *Worker) Get(ctx context.Context, in *work.File) (*work.File, error) {
	file_name := in.GetFileName()
	file, err := ioutil.ReadFile(file_name)
	if err != nil {
		log.Fatalf("[FAIL] Unable to retrieve file %v: %v", file_name, err)
	}
	msg := work.File{File: file}
	return &msg, nil
}

func (w *Worker) Delete(ctx context.Context, in *work.File) (*work.Ack, error) {
	file_name := in.GetFileName()
	var err error
	for i := 0; i < lock_timer; i++ {
		err = os.Remove(file_name)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		log.Printf("[FAIL] Unable to delete file %v: %v", file_name, err)
		return nil, err
	}
	return &done, nil
}

func (w *Worker) GetVersion(ctx context.Context, in *work.FileVersions) (*work.File, error) {
	file_name := in.GetFileName()
	versions := int(in.GetVersions())
	file, err := ioutil.ReadFile(file_name)
	if err != nil {
		log.Fatalf("[FAIL] Unable to retrieve file %v: %v", file_name, err)
	}
	file_str := string(file)
	//fmt.Printf("[DEBUG] File string:\n%v\n", file_str)

	// Algorithm:
	//	- Find the (n + 1)-th occurence of a delimiter
	//		- Keep track of the last delimiter index
	//		- Search for the next delimiter in the substring after the prev delimiter

	version_idx := utils.FindNth(file_str, delimiter, versions)
	out_file_str := string(file_str[0:version_idx])
	//fmt.Printf("[DEBUG] Out file string:\n%v\n", out_file_str)
	out_file := []byte(out_file_str)
	out_file_msg := work.File{File: out_file}
	return &out_file_msg, nil
}

func (w *Worker) Train(ctx context.Context, in *work.Job) (*work.Ack, error) {
	id := strconv.Itoa(int(in.GetJobId()))
	_, err := exec.Command("python3", "train.py", id).Output()
	if err != nil {
		log.Printf("[FAIL] Unable to train model %v: %v", id, err)
		return nil, err
	}
	//log.Printf("[DEBUG] Output: %v", string(out))
	//log.Printf("[SUCCESS] Trained model %v", id)
	return &done, nil
}

/*
func (w *Worker) Inference(ctx context.Context, in *work.Query) (*work.QueryResults, error) {
	jobs := in.GetJobId()
	model := strconv.Itoa(int(in.GetModelId()))
	var results []byte
	for _, id := range jobs {
		job_id := strconv.Itoa(int(id))
		out, err := exec.Command("python3", "inference.py", model, job_id).Output()
		if err != nil {
			log.Printf("[FAIL] Inference on model %v, job %v", model, job_id)
			return nil, err
		}
		results = append(results, out...)
	}
	result_msg := work.QueryResults{Results: results}
	return &result_msg, nil
}
*/

func (w *Worker) Inference(server work.Worker_InferenceServer) error {
	ctx := server.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		recv, err := server.Recv()
		if err != nil {
			//log.Printf("[DEBUG] Inference service done")
			return err
		}
		jobs := recv.GetJobId()
		model_id := strconv.Itoa(int(recv.GetModelId()))
		var results []byte
		var times []float32
		for _, job_id := range jobs {
			job_id := strconv.Itoa(int(job_id))
			start := time.Now()
			out, err := exec.Command("python3", "inference.py", model_id, job_id).Output()
			if err != nil {
				log.Printf("[FAIL] Inference on model %v, job %v", model_id, job_id)
				return err
			}
			duration := time.Since(start) / time.Millisecond
			results = append(results, out...)
			times = append(times, float32(duration))
		}
		out := work.QueryResults{Results: results, Times: times}
		server.Send(&out)
	}
}

// NOT A GRPC SERVICE
func (w *Worker) Join(coordinator_ip string) error {
	stream, err := GetStream(context.Background(), coordinator_ip) // Helper
	// Potentially fetch from another machine instead of passing as parameter
	w.coordinator_ip = coordinator_ip
	if err != nil {
		log.Printf("[FAIL] Unable to fetch stream to %v", coordinator_ip)
		return err
	}
	log.Print("[SUCCESS] Joined the network")
	w.BeginHeartbeating(stream)
	return nil
}

func (w *Worker) ElectStandby(ctx context.Context, in *work.Empty) (*work.Ack, error) {
	// Pipeline for Standby Election
	// start coordinator server
	// begin heartbeating to active coordinator
	//	- if active dies, set this to active
	new_coordinator := c.NewCoordinator()
	// !!! NOT NECESSARY WHEN TESTING ON VMS !!!
	//fmt.Printf("[DEBUG] New Coordinator standby: %v\n", new_coordinator.Standby)
	/*
		new_coordinator.DeleteMember(new_coordinator.Address)
		new_coordinator.Address = utils.GetOutboundIP() + worker_port // worker_port defined in helper file
		new_coordinator.AddMember(new_coordinator.Address)
	*/
	new_coordinator.StartServer(coordinator_port)

	stream, err := GetStream(context.Background(), w.coordinator_ip)
	if err != nil {
		log.Printf("[FAIL] Unable to get standby stream: %v", err)
		return nil, err
	}
	new_coordinator.BeginHeartbeating(stream, w.coordinator_ip)

	return &done, nil
}

func (w *Worker) UpdateActive(ctx context.Context, in *work.Coordinator) (*work.Ack, error) {
	active := in.GetIp()
	w.coordinator_ip = active
	//log.Printf("[DEBUG] Attempting to set coordinator ip to %v", active)
	if err := w.Join(active); err != nil {
		log.Printf("[FAIL] Unable to join new active coordinator %v: %v", active, err)
		return nil, err
	}
	log.Printf("[SUCCESS] Active coordinator updated to %v", active)
	return &done, nil
}

func (w *Worker) GetActive(ctx context.Context, in *work.Empty) (*work.Coordinator, error) {
	coordinator := work.Coordinator{Ip: w.coordinator_ip}
	return &coordinator, nil
}

func (w *Worker) RestartInference(ctx context.Context, in *work.InferenceRequest) (*work.Ack, error) {
	num_queries := in.GetNumQueries()
	coordinator_ip := in.GetCoordinator() // Expected to have port
	conn, err := grpc.Dial(coordinator_ip, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial coordinator: %v\n%v", coordinator_ip, err)
		return nil, err
	}
	client := coord.NewCoordinatorClient(conn)
	test := coord.Query{NumQueries: num_queries, Ip: utils.GetOutboundIP() + worker_port}
	stream, err := client.Inference(context.Background())
	if err != nil {
		log.Printf("[FAIL] Unable to get stream from coordinator: %v", err)
		return nil, err
	}
	stream.Send(&test)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		completed := 0
		var output []byte
		for completed < int(num_queries) {
			//fmt.Print("[DEBUG] Awaiting response...\n")
			resp, err := stream.Recv()

			if err != nil {
				//log.Printf("[DEBUG] Inference finished: %v", err)
				break
			}
			result := resp.GetResults()
			if len(result) == 0 {
				continue
			}
			output = append(output, result...)
			completed++
			time.Sleep(time.Second)
		}
		str_out := string(output)
		if strings.Count(str_out, "\n") < int(num_queries) {
			return
		}
		fmt.Printf("Query results: \n%v", str_out)
	}()
	wg.Wait()
	return &done, nil
}
