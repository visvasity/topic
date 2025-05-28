// Copyright (c) 2019 BVK Chaitanya

package topic

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func TestTopic(t *testing.T) {
	ctx := context.Background()

	topic := New[int64]()
	defer topic.Close()

	var wg sync.WaitGroup
	defer wg.Wait()

	numMembers := 5
	for i := 0; i < numMembers; i++ {
		i := i

		receiver, err := Subscribe(topic, 0, false)
		if err != nil {
			t.Fatal(err)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			var err error
			for msg := range receiver.All(ctx, &err) {
				t.Logf("%d: received %+v", i, msg)
			}
			if err != nil {
				t.Fatal(err)
			}

			receiver.Close()
		}()
	}

	sch, err := SendCh(topic)
	if err != nil {
		t.Fatal(err)
	}

	numMsgs := 5
	for i := 0; i < numMsgs; i++ {
		sch <- rand.Int63()
	}

	if err := topic.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Subscribe(topic, 0, false); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("want os.ErrClosed, got %v", err)
	}
}

func TestEmptyTopic(t *testing.T) {
	topic := New[int64]()
	defer topic.Close()

	sch, err := SendCh(topic)
	if err != nil {
		t.Fatal(err)
	}

	numMsgs := 5
	for i := 0; i < numMsgs; i++ {
		sch <- rand.Int63()
	}
}

func TestQueueLimitRecent(t *testing.T) {
	topic := New[int]()
	defer topic.Close()

	sch, err := SendCh(topic)
	if err != nil {
		t.Fatal(err)
	}

	sub, err := Subscribe(topic, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	numMsgs := 5
	for i := 1; i <= numMsgs; i++ {
		sch <- i
	}

	if v, err := sub.Receive(); err != nil {
		t.Fatal(err)
	} else if v != numMsgs {
		t.Fatalf("want %d, got %d", numMsgs, v)
	}

	sub.Close()

	if _, err := sub.Receive(); err == nil {
		t.Fatalf("want non-nil error, got nil")
	}
}

func TestQueueLimitOldest(t *testing.T) {
	topic := New[int]()
	defer topic.Close()

	sch, err := SendCh(topic)
	if err != nil {
		t.Fatal(err)
	}

	sub, err := Subscribe(topic, -1, false)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	numMsgs := 5
	for i := 1; i <= numMsgs; i++ {
		sch <- i
	}

	if v, err := sub.Receive(); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatalf("want %d, got %d", 1, v)
	}

	sub.Close()

	if _, err := sub.Receive(); err == nil {
		t.Fatalf("want non-nil error, got nil")
	}
}

func TestIncludeRecent(t *testing.T) {
	topic := New[int]()
	defer topic.Close()

	sch, err := SendCh(topic)
	if err != nil {
		t.Fatal(err)
	}

	sub1, err := Subscribe(topic, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer sub1.Close()

	for i := 0; i < 5; i++ {
		sch <- i
	}

	sub2, err := Subscribe(topic, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer sub2.Close()

	for i := 5; i < 10; i++ {
		sch <- i
	}

	if v, err := sub1.Receive(); err != nil {
		t.Fatal(err)
	} else if v != 0 {
		t.Fatalf("want %d, got %d", 0, v)
	}

	if v, err := sub2.Receive(); err != nil {
		t.Fatal(err)
	} else if v != 4 {
		t.Fatalf("want %d, got %d", 4, v)
	}
}

func TestReceiveClose(t *testing.T) {
	topic := New[int]()
	defer topic.Close()

	sub, err := Subscribe(topic, 0, false /* includeLast */)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		time.Sleep(5 * time.Millisecond)
		topic.Close()
	}()

	if _, err := sub.Receive(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("wanted os.ErrClosed, got %v", err)
	}
}

func TestReceiveUnsubscribe(t *testing.T) {
	topic := New[int]()
	defer topic.Close()

	sub, err := Subscribe(topic, 0, false /* includeLast */)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		time.Sleep(5 * time.Millisecond)
		sub.Close()
	}()

	if _, err := sub.Receive(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("wanted os.ErrClosed, got %v", err)
	}
}
