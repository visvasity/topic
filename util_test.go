// Copyright (c) 2023 BVK Chaitanya

package topic

import (
	"testing"
	"time"
)

func TestRecent(t *testing.T) {
	topic := New[int]()
	defer topic.Close()

	if _, ok := topic.Last(); ok {
		t.Fatalf("want false, got true")
	}

	ch, err := SendCh(topic)
	if err != nil {
		t.Fatal(err)
	}

	numMsgs := 5
	for i := 1; i <= numMsgs; i++ {
		ch <- i
	}

	if v, ok := topic.Last(); !ok {
		t.Fatalf("want true, got false")
	} else if v != numMsgs {
		t.Fatalf("want %d, got %d", numMsgs, v)
	}
}

func TestReceiveCh(t *testing.T) {
	topic := New[int]()
	defer topic.Close()

	sub, err := Subscribe(topic, 0, false /* includeRecent */)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := topic.Send(1); err != nil {
		t.Fatal(err)
	}
	if v, err := sub.Receive(); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatalf("want 1, got %d", v)
	}

	if err := topic.Send(2); err != nil {
		t.Fatal(err)
	}
	rch, err := ReceiveCh(sub)
	if err != nil {
		t.Fatal(err)
	}
	if v := <-rch; v != 2 {
		t.Fatalf("want 2, got %d", v)
	}
}

func TestReceiveChClose(t *testing.T) {
	topic := New[int]()
	defer topic.Close()

	sub, err := Subscribe(topic, 0, false /* includeRecent */)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	rch, err := ReceiveCh(sub)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if err := topic.Send(i); err != nil {
			t.Fatal(err)
		}
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		topic.Close()
	}()

	// rch must breakout after topic is closed.
	for v := range rch {
		t.Log(v)
	}
}

func TestReceiveChUnsubscribe(t *testing.T) {
	topic := New[int]()
	defer topic.Close()

	sub, err := Subscribe(topic, 0, false /* includeRecent */)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	rch, err := ReceiveCh(sub)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if err := topic.Send(i); err != nil {
			t.Fatal(err)
		}
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		sub.Close()
	}()

	// rch must breakout after topic is closed.
	for v := range rch {
		t.Log(v)
	}
}
