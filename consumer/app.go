package main

import (
	"fmt"
	"time"
)

func main() {
	fmt.Println("Hello, World!")

	for {
		time.Sleep(180 * time.Second)
	}
}
