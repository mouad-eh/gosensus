package main

import (
	"context"
	"fmt"
	"time"
)

func EventLoop(ctx context.Context, ch <-chan time.Time) {
	// this while loop will not wast CPU cycles
	// because it will block on the channel until
	// a new event is received (this is due to how select is implemented)
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-ch:
			// Do something with event
			fmt.Println(event)
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	go EventLoop(ctx, ticker.C)

	time.Sleep(6 * time.Second)
	cancel()
}
