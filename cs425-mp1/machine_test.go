package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"testing"
)

type grepData struct {
	query  string
	result int
}

func TestMain(t *testing.T) {
	generateLog()
	cmd := exec.Command("go run machine.go")
	input := ":6000\n"
	logFile := " testLog.txt"

	tests := []grepData{
		{"Hello, world!", 10},
		{"Hello world!", 0},
		{"100", 8},
		{"500", 4},
		{"700", 2},
	}

	for _, data := range tests {
		input = data.query + logFile
		cmd.Stdin = strings.NewReader(input)
		output, _ := cmd.Output()
		if string(output) >= fmt.Sprint(data.result) {
			t.Errorf("Main FAILED. Expected %v. Got %v", data.result, string(output))
		}
	}

}

func generateLog() {
	file, err := os.OpenFile("testLog.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	log.SetOutput(file)
	log.Println("Hello, world!")
	log.Println("This is the second line.")
	log.Println("Foo bar")
	for i := 0; i < 10; i++ {
		log.Println(rand.Intn(1000))
	}
}
