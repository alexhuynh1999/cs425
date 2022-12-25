package main

import (
	"log"
	"os/exec"
)

func main() {
	err := exec.Command("python3", "scripts/train.py", "1").Run()
	if err != nil {
		log.Printf("[FAIL] Failed to run: %v", err)
		return
	}
	log.Printf("[SUCCESS] Ran successfully")
}
