// Copyright (c) 2023 BVK Chaitanya

package topic

import "testing"

func TestRecent(t *testing.T) {
	topic := New[int]()
	defer topic.Close()

	if _, ok := Recent(topic); ok {
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

	if v, ok := Recent(topic); !ok {
		t.Fatalf("want true, got false")
	} else if v != numMsgs {
		t.Fatalf("want %d, got %d", numMsgs, v)
	}
}
