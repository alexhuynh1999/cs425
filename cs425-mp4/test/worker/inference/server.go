package main

import (
	"context"
	work "cs425-mp4/proto/worker"
	w "cs425-mp4/structs/worker"
	"io/ioutil"
	"log"

	"google.golang.org/grpc"
)

func main() {
	port := ":6000"
	this := w.NewWorker(port)
	this.StartServer(port)

	conn, err := grpc.Dial("localhost:6000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("[FAIL] Unable to dial worker\n%v", err)
	}

	// Train
	client := work.NewWorkerClient(conn)
	job := work.Job{
		JobId: 1,
	}
	_, err = client.Train(context.Background(), &job)
	if err != nil {
		log.Printf("[FAIL] Unable to train model: %v", err)
		return
	}
	log.Print("[SUCCESS] Trained model id 1")

	// Inference
	local_name := "query.jpg"
	file, err := ioutil.ReadFile(local_name)
	if err != nil {
		log.Printf("[FAIL] Unable to read file %v: %v", local_name, err)
	}
	query := work.Query{
		JobId: 1,
		Image: file,
	}
	resp, err := client.Inference(context.Background(), &query)
	if err != nil {
		log.Printf("[FAIL] Unable to infer on model %v", 1)
	}
	log.Printf("[SUCCESS] Inference has result: %v", resp.GetResults())
}
