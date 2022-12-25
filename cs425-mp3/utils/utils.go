package utils

import (
	"log"
	"net"
)

func GetLeftExclusive(left map[string]int, right map[string]int) []string {
	var out []string
	for key := range left {
		if _, exists := right[key]; !exists {
			out = append(out, key)
		}
	}

	return out
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
