// Copyright (c) 2025 Visvasity LLC

package topic

import (
	"math/rand"
	"testing"
)

func Benchmark1Send1Receive(b *testing.B) {
	topic := New[int64]()
	defer topic.Close()

	receiver, err := Subscribe(topic, 0, true /* includeRecent */)
	if err != nil {
		b.Fatal(err)
	}
	defer receiver.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic.Send(rand.Int63())
		if _, err := receiver.Receive(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func Benchmark1Send1ReceiveCh(b *testing.B) {
	topic := New[int64]()
	defer topic.Close()

	receiver, err := Subscribe(topic, 0, true /* includeRecent */)
	if err != nil {
		b.Fatal(err)
	}
	defer receiver.Close()

	rch, err := ReceiveCh(receiver)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic.Send(int64(i))
		<-rch
	}
	b.StopTimer()
}

func Benchmark1Send10Receives(b *testing.B) {
	topic := New[int64]()
	defer topic.Close()

	nreceivers := 10
	var receivers []*Receiver[int64]
	for i := 0; i < nreceivers; i++ {
		r, err := Subscribe(topic, 0, true /* includeRecent */)
		if err != nil {
			b.Fatal(err)
		}
		defer r.Close()

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

func Benchmark1Send10ReceiveCh(b *testing.B) {
	topic := New[int64]()
	defer topic.Close()

	nreceivers := 10
	var receivers []*Receiver[int64]
	for i := 0; i < nreceivers; i++ {
		r, err := Subscribe(topic, 0, true /* includeRecent */)
		if err != nil {
			b.Fatal(err)
		}
		defer r.Close()

		receivers = append(receivers, r)
	}

	var rchs []<-chan int64
	for _, r := range receivers {
		rch, err := ReceiveCh(r)
		if err != nil {
			b.Fatal(err)
		}
		rchs = append(rchs, rch)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic.Send(rand.Int63())
		for _, rch := range rchs {
			<-rch
		}
	}
	b.StopTimer()
}
