// Copyright (c) 2023 BVK Chaitanya

package topic

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"testing"
)

func TestStress(t *testing.T) {
	ctx := context.Background()

	const numReceivers = 10
	const numMessages = 100000

	topic := New[int64]()
	defer topic.Close()

	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(numReceivers)
	for i := 0; i < numReceivers; i++ {
		receiver, err := Subscribe(topic, 0, true /* includeRecent */)
		if err != nil {
			t.Fatal(err)
		}

		go func(id int) {
			n := 0
			for v := range receiver.All(ctx, &err) {
				_ = v
				n++
			}
			receiver.Unsubscribe()
			log.Printf("%d: received %d messages", id, n)
			wg.Done()
		}(i)
	}

	for i := 0; i < numMessages; i++ {
		topic.Send(rand.Int63())
	}
	topic.Close()
}
