package main

import (
	"context"
	"log"

	work "cs425-mp4/proto/worker"

	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("localhost:6000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("[FAIL] Unable to dial worker\n%v", err)
	}

	client := work.NewWorkerClient(conn)
	job := work.Job{
		JobId: 2,
	}
	_, err = client.Train(context.Background(), &job)
	if err != nil {
		log.Printf("[FAIL] Unable to train model: %v", err)
		return
	}
	log.Print("[SUCCESS] Trained model id 1")
}
