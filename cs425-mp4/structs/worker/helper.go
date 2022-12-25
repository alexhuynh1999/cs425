package worker

import (
	"context"
	"log"
	"net"
	"time"

	coord "cs425-mp4/proto/coordinator"
	work "cs425-mp4/proto/worker"
	utils "cs425-mp4/utils"

	"google.golang.org/grpc"
)

var (
	coordinator_port = ":6000"
	worker_port      = ":7000"
)

func NewWorker(port string) Worker {
	if port == "" {
		port = worker_port
	}
	machine := Worker{ip: utils.GetOutboundIP() + port}
	return machine
}

func (w *Worker) SetActive(address string) {
	w.coordinator_ip = address
}

func (w *Worker) GetCoordinator() string {
	return w.coordinator_ip
}

func GetStream(ctx context.Context, ip string) (coord.Coordinator_JoinClient, error) {
	conn, err := grpc.Dial(ip, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial coordinator: %v\n%v", ip, err)
		return nil, err
	}

	client := coord.NewCoordinatorClient(conn)
	stream, err := client.Join(ctx)
	if err != nil {
		log.Printf("[FAIL] Unable to fetch stream from %v: %v", ip, err)
		return nil, err
	}
	return stream, nil
}

func (w *Worker) GetIp() string {
	return w.ip
}

func (w *Worker) StartServer(port string) {
	go func() {
		lis, err := net.Listen("tcp", port)
		if err != nil {
			log.Fatalf("[FAIL] Unable to listen on port: %v\n%v", port, err)
		}

		server := grpc.NewServer()
		work.RegisterWorkerServer(server, w)
		log.Printf("[DEBUG] Worker server now serving")
		if err := server.Serve(lis); err != nil {
			log.Fatalf("[FAIL] Unable to serve\n%v", err)
		}
	}()
}

func (w *Worker) BeginHeartbeating(stream coord.Coordinator_JoinClient) {
	go func() {
		if err := w.heartbeat(stream); err != nil {
			log.Printf("[FAIL] Unable to heartbeat to leader: %v", err)
			return
		}
		for {
			if err := w.heartbeat(stream); err != nil {
				log.Printf("[WARNING] Worker unable to heartbeat to leader %v", err)
				return
			}
			time.Sleep(time.Second)
		}
	}()
}

func (w *Worker) heartbeat(stream coord.Coordinator_JoinClient) error {
	hb := coord.Heartbeat{
		Ip: w.GetIp(),
	}
	//log.Print("[DEBUG] Heartbeat to leader")
	return stream.Send(&hb)
}

/*
func (w *Worker) SetIp(new_ip string) {
	// IP SETTER FOR TESTING PURPOSES
	w.ip = new_ip
}
*/
