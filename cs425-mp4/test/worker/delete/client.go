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
	get_file := work.File{
		FileName: "sdfs1",
	}
	resp, err := client.Delete(context.Background(), &get_file)
	if err != nil {
		log.Printf("[FAIL] Unable to get file: %v", err)
	}
	done := resp.GetDone()
	if done {
		log.Printf("[SUCCESS] Deleted file sdfs1")
	}
}
