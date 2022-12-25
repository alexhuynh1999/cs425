package main

import (
	"fmt"
	"sync"
	"time"
)

type Clock struct {
	sync.Mutex
	vals chan float32
	time chan int64
}

func NewClock() Clock {
	c := Clock{
		vals: make(chan float32, 2000),
		time: make(chan int64, 2000),
	}
	c.StartTicking()
	return c
}

func (c *Clock) StartTicking() {
	go func() {
		for {
			c.vals <- 0.0
			c.time <- time.Now().Unix()
			time.Sleep(time.Second)
		}
	}()
}

func (c *Clock) Add(n float32) {
	go func() {
		c.vals <- n
		c.time <- time.Now().Unix()
		//fmt.Printf("%v\n", time.Now().Unix())
	}()
}

func (c *Clock) GetRate(seconds int) float32 {
	now := time.Now().Unix()
	var sum float32
	for {
		val := <-c.vals
		timestamp := <-c.time
		since := now - timestamp
		if since > 2 {
			continue
		}
		if since < 0 {
			break
		}
		sum += val
	}
	fmt.Printf("%v\n", sum/10.0)
	return sum / 10.0
}

func main() {
	c := NewClock()
	c.Add(3.14159)
	time.Sleep(time.Second * 5)
	c.Add(3.14)
	go c.GetRate(10)
	for {

	}
}
