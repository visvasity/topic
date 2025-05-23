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
		receiver, err := topic.Subscribe(0, true /* includeRecent */)
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

func Benchmark1Send1Receive(b *testing.B) {
	topic := New[int64]()
	defer topic.Close()

	receiver, err := topic.Subscribe(0, true /* includeRecent */)
	if err != nil {
		b.Fatal(err)
	}
	defer receiver.Unsubscribe()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic.Send(rand.Int63())
		if _, err := receiver.Receive(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func Benchmark1Send10Receives(b *testing.B) {
	topic := New[int64]()
	defer topic.Close()

	nreceivers := 10
	var receivers []*Receiver[int64]
	for i := 0; i < nreceivers; i++ {
		r, err := topic.Subscribe(0, true /* includeRecent */)
		if err != nil {
			b.Fatal(err)
		}
		defer r.Unsubscribe()

		receivers = append(receivers, r)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic.Send(rand.Int63())
		for _, r := range receivers {
			if _, err := r.Receive(); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
}
