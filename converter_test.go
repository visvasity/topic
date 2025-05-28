// Copyright (c) 2025 Visvasity LLC

package topic

import (
	"math/rand"
	"testing"
)

func TestSubscribeFunc(t *testing.T) {
	topic := New[int32]()
	defer topic.Close()

	fn := func(x int32) int64 { return int64(x) * 100 }
	sub, err := SubscribeFunc(topic, fn, 0, false)
	if err != nil {
		t.Fatal(err)
	}

	numMsgs := 50
	for i := 0; i < numMsgs; i++ {
		topic.Send(rand.Int31())
	}

	for i := 0; i < numMsgs; i++ {
		x, err := sub.Receive()
		if err != nil {
			t.Fatal(err)
		}
		if x%100 != 0 {
			t.Errorf("wanted multiple of 100, got %d", x)
		}
	}
}
