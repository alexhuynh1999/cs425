package main

import (
	"context"
	"io/ioutil"
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
	b, err := ioutil.ReadFile("test_file")
	if err != nil {
		log.Fatalf("[FAIL] Unable to read file: %v", err)
	}
	send_file := work.File{FileName: "sdfs1", File: b}
	resp, err := client.Put(context.Background(), &send_file)
	if err != nil {
		log.Printf("[FAIL] Placing file: %v", err)
	}
	if resp.GetDone() == true {
		log.Printf("[SUCCESS] Placed file")
	}
}
