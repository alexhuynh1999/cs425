package client

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"time"

	c "cs425-mp4/proto/coordinator"
	w "cs425-mp4/proto/worker"
	utils "cs425-mp4/utils"

	"google.golang.org/grpc"
)

var (
	coordinator_port = ":6000"
	worker_port      = ":7000"
	vms              = []string{
		"172.22.95.22" + worker_port,
		"172.22.157.23" + worker_port,
		"172.22.159.23" + worker_port,
		"172.22.95.23" + worker_port,
		"172.22.157.24" + worker_port,
	}
)

func GetActive() (string, error) {
	var err error
	for _, vm := range vms {
		//fmt.Printf("[DEBUG] Dialing %v\n", vm)
		conn, err := grpc.Dial(vm, grpc.WithInsecure())
		if err != nil {
			continue
		}
		client := w.NewWorkerClient(conn)
		empty := w.Empty{}
		resp, err := client.GetActive(context.Background(), &empty)
		if err != nil {
			continue
		}
		active := resp.GetIp()
		//fmt.Printf("[DEBUG] Active: %v\n", active)
		active = utils.StripPort(active) + coordinator_port
		return active, nil
	}
	return "", err
}

func GetMembers() {
	coordinator_ip, err := GetActive()
	if err != nil {
		log.Printf("[FAIL] Unable to get active coordinator")
		return
	}
	conn, err := grpc.Dial(coordinator_ip, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial coordinator: %v\n%v", coordinator_ip, err)
		return
	}
	client := c.NewCoordinatorClient(conn)
	resp, err := client.GetMembers(context.Background(), &c.Empty{})
	if err != nil {
		log.Printf("[FAIL] Unable to get members from coordinator: %v\n%v", coordinator_ip, err)
		return
	}
	ips := resp.GetIps()
	vms := resp.GetVms()
	for i, ip := range ips {
		vm := vms[i]
		fmt.Printf("VM: %v @ %v\n", vm, ip)
	}
}

func Put(local_name string, sdfs_name string) {
	coordinator_ip, err := GetActive()
	if err != nil {
		log.Printf("[FAIL] Unable to get active coordinator")
		return
	}
	conn, err := grpc.Dial(coordinator_ip, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("[FAIL] Unable to dial coordinator: %v\n%v", coordinator_ip, err)
	}
	client := c.NewCoordinatorClient(conn)
	file, err := ioutil.ReadFile(local_name)
	if err != nil {
		log.Printf("[FAIL] Unable to read file %v: %v", local_name, err)
	}
	file_msg := c.File{FileName: sdfs_name, File: file}
	resp, err := client.Put(context.Background(), &file_msg)
	if err != nil {
		log.Printf("[FAIL] Unable to put file %v: %v", sdfs_name, err)
		return
	}
	done := resp.GetDone()
	if done {
		log.Printf("[SUCCESS] Put file %v as %v in network", local_name, sdfs_name)
	}
}

func Get(sdfs_name string, local_name string) {
	coordinator_ip, err := GetActive()
	if err != nil {
		log.Printf("[FAIL] Unable to get active coordinator")
		return
	}
	conn, err := grpc.Dial(coordinator_ip, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("[FAIL] Unable to dial coordinator: %v\n%v", coordinator_ip, err)
	}
	client := c.NewCoordinatorClient(conn)
	request := c.File{FileName: sdfs_name}
	resp, err := client.Get(context.Background(), &request)
	if err != nil {
		log.Printf("[FAIL] Unable to get file %v: %v", sdfs_name, err)
		return
	}
	file := resp.GetFile()
	err = ioutil.WriteFile(local_name, file, 0664)
	if err != nil {
		log.Printf("[FAIL] Unable to write to file %v: %v", local_name, err)
		return
	}
	log.Printf("[SUCCESS] Get operation: file %v from network stored into local file %v", sdfs_name, local_name)

}

func Delete(sdfs_name string) {
	coordinator_ip, err := GetActive()
	if err != nil {
		log.Printf("[FAIL] Unable to get active coordinator")
		return
	}
	conn, err := grpc.Dial(coordinator_ip, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("[FAIL] Unable to dial coordinator: %v\n%v", coordinator_ip, err)
	}
	client := c.NewCoordinatorClient(conn)
	del_request := c.File{FileName: sdfs_name}
	_, err = client.Delete(context.Background(), &del_request)
	if err != nil {
		log.Printf("[FAIL] Unable to delete file %v: %v", sdfs_name, err)
		return
	}
	log.Printf("[SUCCESS] Delete file %v from the network", sdfs_name)
}

func ListFiles(sdfs_name string) {
	coordinator_ip, err := GetActive()
	if err != nil {
		log.Printf("[FAIL] Unable to get active coordinator")
		return
	}
	conn, err := grpc.Dial(coordinator_ip, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial coordinator: %v\n%v", coordinator_ip, err)
		return
	}
	client := c.NewCoordinatorClient(conn)
	file_request := c.File{FileName: sdfs_name}
	resp, err := client.ListFiles(context.Background(), &file_request)
	if err != nil {
		log.Printf("[FAIL] Unable to get locations of file %v: %v", sdfs_name, err)
		return
	}
	ips := resp.GetIps()
	vms := resp.GetVms()
	for i, ip := range ips {
		vm := vms[i]
		fmt.Printf("VM: %v @ %v\n", vm, ip)
	}
}

func Store() {
	coordinator_ip, err := GetActive()
	if err != nil {
		log.Printf("[FAIL] Unable to get active coordinator")
		return
	}
	conn, err := grpc.Dial(coordinator_ip, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial coordinator: %v\n%v", coordinator_ip, err)
		return
	}
	client := c.NewCoordinatorClient(conn)
	store_req := c.Heartbeat{Ip: utils.GetOutboundIP() + worker_port}
	resp, err := client.Store(context.Background(), &store_req)
	if err != nil {
		log.Printf("[FAIL] Unable to determine what fail is stored here: %v", err)
		return
	}
	files := resp.GetFiles()
	fmt.Print("Files stored at this machine:\n")
	for _, file := range files {
		fmt.Printf("%v\n", file)
	}
}

func GetVersions(sdfs_name string, local_name string, versions string) {
	coordinator_ip, err := GetActive()
	if err != nil {
		log.Printf("[FAIL] Unable to get active coordinator")
		return
	}
	conn, err := grpc.Dial(coordinator_ip, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("[FAIL] Unable to dial coordinator: %v\n%v", coordinator_ip, err)
	}
	client := c.NewCoordinatorClient(conn)
	versions_int, _ := strconv.Atoi(versions)
	request := c.FileVersions{FileName: sdfs_name, Versions: int32(versions_int)}
	resp, err := client.GetVersion(context.Background(), &request)
	if err != nil {
		log.Printf("[FAIL] Unable to get file %v: %v", sdfs_name, err)
		return
	}
	file := resp.GetFile()
	err = ioutil.WriteFile(local_name, file, 0664)
	if err != nil {
		log.Printf("[FAIL] Unable to write to file %v: %v", local_name, err)
		return
	}
	log.Printf("[SUCCESS] Get operation: file %v from network stored into local file %v", sdfs_name, local_name)
}

func Train(batch_size int32) {
	if batch_size <= 0 {
		fmt.Print("Invalid batch size. Try again\n")
		return
	}
	coordinator_ip, err := GetActive()
	if err != nil {
		log.Printf("[FAIL] Unable to get active coordinator")
		return
	}
	conn, err := grpc.Dial(coordinator_ip, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial coordinator: %v\n%v", coordinator_ip, err)
		return
	}
	client := c.NewCoordinatorClient(conn)
	train_msg := c.Hyperparameters{BatchSize: batch_size}
	_, err = client.Train(context.Background(), &train_msg)
	if err != nil {
		log.Printf("[FAIL] Unable to train models with batch size %v: %v", batch_size, err)
		return
	}
	log.Printf("[SUCCESS] Train models with batch size %v", batch_size)
}

/*
func Inference(num_queries int32) {
	coordinator_ip, err := GetActive()
	if err != nil {
		log.Printf("[FAIL] Unable to get active coordinator")
		return
	}
	conn, err := grpc.Dial(coordinator_ip, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial coordinator: %v\n%v", coordinator_ip, err)
		return
	}
	client := c.NewCoordinatorClient(conn)
	query := c.Query{NumQueries: num_queries}
	resp, err := client.Inference(context.Background(), &query)
	if err != nil {
		log.Printf("[FAIL] Unable to inference %v queries: %v", num_queries, err)
		return
	}
	results := string(resp.GetResults())
	log.Printf("[SUCCESS] Inference results: \n%v", results)
}
*/

func Inference(num_queries int32) {
	coordinator_ip, err := GetActive()
	if err != nil {
		log.Printf("[FAIL] Unable to get active coordinator")
		return
	}
	conn, err := grpc.Dial(coordinator_ip, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial coordinator: %v\n%v", coordinator_ip, err)
		return
	}
	client := c.NewCoordinatorClient(conn)
	test := c.Query{NumQueries: num_queries, Ip: utils.GetOutboundIP() + worker_port}
	stream, err := client.Inference(context.Background())
	if err != nil {
		log.Printf("[FAIL] Unable to get stream from coordinator: %v", err)
		return
	}
	stream.Send(&test)
	//fmt.Print("Query results: \n")
	go func() {
		completed := 0
		//var output []byte
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
			//output = append(output, result...)
			//fmt.Print(string(result))
			completed++
			time.Sleep(time.Second)
		}
	}()
}

func GetRate() {
	coordinator_ip, err := GetActive()
	if err != nil {
		log.Printf("[FAIL] Unable to get active coordinator")
		return
	}
	conn, err := grpc.Dial(coordinator_ip, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial coordinator: %v\n%v", coordinator_ip, err)
		return
	}
	client := c.NewCoordinatorClient(conn)
	model1 := c.Model{ModelId: 0}
	resp1, err := client.GetRate(context.Background(), &model1)
	if err != nil {
		log.Printf("[FAIL] Unable to get rate of model %v: %v", 0, err)
		return
	}
	model2 := c.Model{ModelId: 1}
	resp2, err := client.GetRate(context.Background(), &model2)
	if err != nil {
		log.Printf("[FAIL] Unable to get rate of model %v: %v", 1, err)
		return
	}
	fmt.Printf("Model 1:\n")
	fmt.Printf("Query rate: %v\n", resp1.GetRate())
	fmt.Printf("Total processed queries: %v\n", resp1.GetProcessed())
	fmt.Printf("Model 2:\n")
	fmt.Printf("Query rate: %v\n", resp2.GetRate())
	fmt.Printf("Total processed queries: %v\n", resp2.GetProcessed())
}

func Stats(model_id int32) {
	coordinator_ip, err := GetActive()
	if err != nil {
		log.Printf("[FAIL] Unable to get active coordinator")
		return
	}
	conn, err := grpc.Dial(coordinator_ip, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial coordinator: %v\n%v", coordinator_ip, err)
		return
	}
	client := c.NewCoordinatorClient(conn)
	model := c.Model{ModelId: model_id}
	resp, err := client.Stats(context.Background(), &model)
	if err != nil {
		log.Printf("[FAIL] Unable to get stats of model %v: %v", model_id, err)
		return
	}
	fmt.Printf("Mean: %v\n", resp.GetMean())
	fmt.Printf("Median: %v\n", resp.GetMedian())
	fmt.Printf("Std: %v\n", resp.GetStd())
	percentiles := resp.GetPercentiles()
	fmt.Printf("90-th Percentile: %v\n", percentiles[0])
	fmt.Printf("95-th Percentile: %v\n", percentiles[1])
	fmt.Printf("99-th Percentile: %v\n", percentiles[2])
}

func SetBatchSize(model_id int32, batch_size int32) {
	coordinator_ip, err := GetActive()
	if err != nil {
		log.Printf("[FAIL] Unable to get active coordinator")
		return
	}
	conn, err := grpc.Dial(coordinator_ip, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial coordinator: %v\n%v", coordinator_ip, err)
		return
	}
	client := c.NewCoordinatorClient(conn)
	model := c.Model{ModelId: model_id, BatchSize: batch_size}
	_, err = client.SetBatchSize(context.Background(), &model)
	if err != nil {
		log.Printf("[FAIL] Unable to get stats of model %v: %v", model_id, err)
		return
	}
	log.Printf("[SUCCESS] Set batch size of model %v to %v", model_id, batch_size)
}

func ShowJobs() {
	coordinator_ip, err := GetActive()
	if err != nil {
		log.Printf("[FAIL] Unable to get active coordinator")
		return
	}
	conn, err := grpc.Dial(coordinator_ip, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial coordinator: %v\n%v", coordinator_ip, err)
		return
	}
	client := c.NewCoordinatorClient(conn)
	empty := c.Empty{}
	resp, err := client.ShowJobs(context.Background(), &empty)
	if err != nil {
		log.Printf("[FAIL] Unable to show jobs: %v", err)
		return
	}
	addresses := resp.GetIps()
	job_msgs := resp.GetJobs()
	for i, address := range addresses {
		vm_ip := utils.StripPort(address) + worker_port
		jobs := job_msgs[i].GetJobIds()
		vm := utils.MapIpsToVms([]string{vm_ip})[0]
		fmt.Printf("VM: %v @ %v Jobs: %v\n", vm, address, jobs)
	}
}

func GetQuery() {
	coordinator_ip, err := GetActive()
	if err != nil {
		log.Printf("[FAIL] Unable to get active coordinator")
		return
	}
	conn, err := grpc.Dial(coordinator_ip, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial coordinator: %v\n%v", coordinator_ip, err)
		return
	}
	client := c.NewCoordinatorClient(conn)
	empty := c.Empty{}
	resp, err := client.GetResults(context.Background(), &empty)
	if err != nil {
		log.Printf("[FAIL] Unable to get results: %v", err)
		return
	}
	result := resp.GetResult()
	fmt.Printf("Query Results: \n%v\n", result)
}
