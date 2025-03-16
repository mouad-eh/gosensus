package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func Timeout(ctx context.Context, ch <-chan time.Time) {
	select {
	case <-ctx.Done():
		fmt.Println("timeout canceled")
	case time := <-ch:
		fmt.Println("timeout at ", time)
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	timeout := time.After(3 * time.Second)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		Timeout(ctx, timeout)
	}()

	wg.Wait()
}
