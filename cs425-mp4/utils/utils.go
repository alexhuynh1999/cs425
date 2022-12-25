package utils

import (
	"log"
	"math/rand"
	"net"
	"reflect"
	"strings"
	"time"
)

var ips_to_vms = map[string]string{
	"172.22.95.22:7000":  "1",
	"172.22.157.23:7000": "2",
	"172.22.159.23:7000": "3",
	"172.22.95.23:7000":  "4",
	"172.22.157.24:7000": "5",
	"172.22.159.24:7000": "6",
	"172.22.95.24:7000":  "7",
	"172.22.157.25:7000": "8",
	"172.22.159.25:7000": "9",
	"172.22.95.25:7000":  "10",
}

func GetKeys(m map[string]string) []string {
	// Gets the keys of a map/dictionary
	keys := reflect.ValueOf(m).MapKeys()
	strkeys := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		strkeys[i] = keys[i].String()
	}
	return strkeys
}

func GetMembers(m map[string]map[string]string) []string {
	// Gets the keys of a map/dictionary
	keys := reflect.ValueOf(m).MapKeys()
	strkeys := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		strkeys[i] = keys[i].String()
	}
	return strkeys
}

func MapIpsToVms(ips []string) []string {
	vms := make([]string, len(ips))
	for i := 0; i < len(ips); i++ {
		ip := ips[i]
		vms[i] = ips_to_vms[ip]
	}
	return vms
}

func GetOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Printf("Couldn't get the IP address of the process\n%v", err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP.String()
}

func FindNth(str string, substr string, n int) int {
	idx := 0
	for m := 0; m < n; m++ {
		curr_idx := strings.Index(str[idx+len(substr):], substr)
		if curr_idx == -1 {
			return len(str) - 1
		}
		idx += curr_idx + len(substr)
		//fmt.Printf("%v\n", idx)
	}

	return idx
}

func GetRandomMember(members []string) string {
	rand.Seed(time.Now().Unix())
	n := rand.Intn(len(members))
	member := members[n]
	return member
}

func StripPort(address string) string {
	port_idx := strings.Index(address, ":")
	if port_idx == -1 {
		//log.Printf("[WARNING] %v has no port", address)
		return address
	}
	return address[:port_idx]
}

func ListToMap(list []string) map[string]string {
	out := make(map[string]string)
	for _, val := range list {
		out[val] = "1"
	}
	return out
}

func GetModels(m map[int]int32) []int {
	keys := reflect.ValueOf(m).MapKeys()
	models := make([]int, len(keys))
	for i := 0; i < len(keys); i++ {
		models[i] = int(keys[i].Int())
	}
	return models
}

func GetAvailableModels(m map[string]map[int]string) (string, int) {
	keys := reflect.ValueOf(m).MapKeys()
	addresses := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		addresses[i] = keys[i].String()
	}

	for _, address := range addresses {
		keys := reflect.ValueOf(m[address]).MapKeys()
		models := make([]int, len(keys))
		for i := 0; i < len(keys); i++ {
			models[i] = int(keys[i].Int())
		}

		for _, model := range models {
			status := m[address][model]
			if status == "a" {
				return address, model
			}

		}
	}
	return "", -1
}
