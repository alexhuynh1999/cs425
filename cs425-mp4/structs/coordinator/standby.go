package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	coord "cs425-mp4/proto/coordinator"
	work "cs425-mp4/proto/worker"
	utils "cs425-mp4/utils"

	"google.golang.org/grpc"
)

var (
	coordinator_port = ":6000"
)

func GetStream(ctx context.Context, address string) (coord.Coordinator_JoinClient, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Printf("[FAIL] Unable to dial coordinator: %v\n%v", address, err)
		return nil, err
	}

	client := coord.NewCoordinatorClient(conn)
	stream, err := client.Join(ctx)
	if err != nil {
		log.Printf("[FAIL] Unable to fetch stream from %v: %v", address, err)
		return nil, err
	}
	return stream, nil
}

func (c *Coordinator) BeginHeartbeating(stream coord.Coordinator_JoinClient, active string) {
	go func() {
		if c.heartbeat(stream) != nil {
			log.Printf("[FAIL] Standby unable to heartbeat to leader")
			return
		}
		for {
			if c.heartbeat(stream) != nil {
				log.Printf("[DEBUG] Standby unable to heartbeat to leader. Setting as active")
				active = utils.StripPort(active) + worker_port
				c.DeleteMember(active)
				c.SetActive()
				// TODO: restart inference job if working
				if c.Status == "working" {
					conn, err := grpc.Dial(c.Client, grpc.WithInsecure())
					if err != nil {
						log.Printf("[FAIL] Unable to dial %v: %v", c.Client, err)
						return
					}
					client := work.NewWorkerClient(conn)
					new_coord_msg := work.InferenceRequest{
						NumQueries:  c.CurrentJobQueries,
						Coordinator: utils.GetOutboundIP() + coordinator_port,
					}
					if utils.StripPort(c.Client) == utils.GetOutboundIP() {
						time.Sleep(time.Second)
					}
					_, err = client.RestartInference(context.Background(), &new_coord_msg)
					if err != nil {
						log.Printf("[FAIL] Unable to restart inference at %v: %v", c.Client, err)
						return
					}
					fmt.Printf("[DEBUG] Restarting inference...\n")
					return
				}
				return
			}
			time.Sleep(1000 * time.Millisecond)
		}
	}()
}

func (c *Coordinator) heartbeat(stream coord.Coordinator_JoinClient) error {
	hb := coord.Heartbeat{
		Ip:                utils.GetOutboundIP() + ":STANDBY",
		MemberListVersion: int32(c.MemberListVersion),
		FileListVersion:   int32(c.FileListVersion),
	}
	return stream.Send(&hb)
}

func (c *Coordinator) UpdateNetworkInfo(ctx context.Context, in *coord.NetworkInfo) (*coord.Ack, error) {
	update_type := in.GetType()
	new_keys := in.GetKeys()
	lsts := in.GetLsts()
	version := in.GetVersion()

	if update_type == "Member" {
		if int(version) > c.MemberListVersion {
			c.MemberList = make(map[string]map[string]string)
			for i, key := range new_keys {
				lst := lsts[i].GetVals()
				c.MemberList[key] = utils.ListToMap(lst)
			}
			c.MemberListVersion = int(version)
		}
		return &done, nil
	} else if update_type == "File" {
		if int(version) > c.FileListVersion {
			c.FileListVersion = int(version)
			c.FileList = make(map[string]map[string]string)
			for i, key := range new_keys {
				lst := lsts[i].GetVals()
				c.FileList[key] = utils.ListToMap(lst)
			}
		}
		return &done, nil
	} else {
		log.Printf("[FAIL] Invalid update type")
		return nil, errors.New("invalid update type")
	}
}

func (c *Coordinator) CreateModel(ctx context.Context, in *coord.ModelInfo) (*coord.Ack, error) {
	batch_size := in.GetBatchSize()
	c.AddModel(batch_size)
	return &done, nil
}

func (c *Coordinator) UpdateModel(ctx context.Context, in *coord.ModelStats) (*coord.Ack, error) {
	update_type := in.GetUpdateType()
	model_id := in.GetModelId()
	model := &c.ModelList[model_id]
	if update_type == "jobs" {
		num_jobs := in.GetNumJobs()
		model.AddJobs(int(num_jobs))
		return &done, nil
	} else if update_type == "times" {
		times := in.GetTime()
		for _, time := range times {
			model.AddTime(float64(time))
		}
		return &done, nil
	} else {
		return nil, errors.New("invalid update type")
	}
}

func (c *Coordinator) UpdateJobStatus(ctx context.Context, in *coord.Status) (*coord.Ack, error) {
	working := in.GetWorking()
	if working {
		num_jobs := in.GetNumQueries()
		client := in.GetClient()
		c.Status = "working"
		c.CurrentJobQueries = num_jobs
		c.Client = client
	} else {
		c.Status = "ready"
		c.CurrentJobQueries = 0
		c.Client = ""
	}
	return &done, nil
}
