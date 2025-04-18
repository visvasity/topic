// Copyright (c) 2025 Visvasity LLC

package topic

import (
	"testing"
	"time"
)

// TestTopicBasics exercises all methods of the topic package: New, Send, SendCh,
// Subscribe, Close, Unsubscribe, and Recent. It tests various scenarios including
// different limit values, concurrent usage, and edge cases.
func TestTopicBasics(t *testing.T) {
	t.Run("BasicPublishSubscribe", func(t *testing.T) {
		// Test basic message sending and receiving with unbounded queue (limit=0).
		topic := New[string]()
		receiver, ch, err := topic.Subscribe(0, false /* includeRecent */)
		if err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		// Send messages via Send and SendCh.
		topic.Send("message1")
		sendCh := topic.SendCh()
		sendCh <- "message2"

		// Receive messages.
		msgs := []string{}
		for i := 0; i < 2; i++ {
			select {
			case msg := <-ch:
				msgs = append(msgs, msg)
			case <-time.After(time.Second):
				t.Fatal("Timed out waiting for message")
			}
		}

		// Verify messages.
		expected := []string{"message1", "message2"}
		for i, msg := range msgs {
			if msg != expected[i] {
				t.Errorf("Expected message %q, got %q", expected[i], msg)
			}
		}

		// Test Recent.
		if msg, ok := Recent(topic); !ok || msg != "message2" {
			t.Errorf("Recent: expected (%q, true), got (%q, %v)", "message2", msg, ok)
		}

		// Unsubscribe and verify channel is closed.
		receiver.Unsubscribe()
		if _, ok := <-ch; ok {
			t.Error("Channel not closed after Unsubscribe")
		}
	})

	t.Run("PositiveLimit", func(t *testing.T) {
		// Test limit > 0 (most recent N messages).
		topic := New[int]()
		receiver, ch, err := topic.Subscribe(2, false /* includeRecent */) // Buffer 2 most recent messages.
		if err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		// Send more messages than buffer size.
		for i := 1; i <= 4; i++ {
			topic.Send(i)
		}

		// Receive messages (should get only the most recent 2: 3 and 4).
		msgs := []int{}
		for i := 0; i < 2; i++ {
			select {
			case msg := <-ch:
				msgs = append(msgs, msg)
			case <-time.After(time.Second):
				t.Fatal("Timed out waiting for message")
			}
		}

		// Verify only recent messages are received.
		expected := []int{3, 4}
		for i, msg := range msgs {
			if msg != expected[i] {
				t.Errorf("Expected message %d, got %d", expected[i], msg)
			}
		}

		receiver.Unsubscribe()
	})

	t.Run("NegativeLimit", func(t *testing.T) {
		// Test limit < 0 (oldest |N| messages).
		topic := New[int]()
		receiver, ch, err := topic.Subscribe(-2, false /* includeRecent */) // Buffer 2 oldest messages.
		if err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		// Send more messages than buffer size.
		for i := 1; i <= 4; i++ {
			topic.Send(i)
		}

		// Receive messages (should get only the oldest 2: 1 and 2).
		msgs := []int{}
		for i := 0; i < 2; i++ {
			select {
			case msg := <-ch:
				msgs = append(msgs, msg)
			case <-time.After(time.Second):
				t.Fatal("Timed out waiting for message")
			}
		}

		// Verify only oldest messages are received.
		expected := []int{1, 2}
		for i, msg := range msgs {
			if msg != expected[i] {
				t.Errorf("Expected message %d, got %d", expected[i], msg)
			}
		}

		receiver.Unsubscribe()
	})

	t.Run("ClosedTopic", func(t *testing.T) {
		// Test behavior with closed topic.
		topic := New[string]()
		if err := topic.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		// Test Subscribe on closed topic.
		if _, _, err := topic.Subscribe(0, false /* includeRecent */); err == nil {
			t.Error("Subscribe on closed topic should return error")
		}

		// Test Send on closed topic.
		topic.Send("test")

		// Test SendCh on closed topic is nil.
		if ch := topic.SendCh(); ch != nil {
			t.Error("SendCh on closed topic should return nil")
		}

		// Test Recent on closed topic.
		if _, ok := Recent(topic); ok {
			t.Error("Recent on closed topic should return false")
		}
	})

	t.Run("EdgeCases", func(t *testing.T) {
		// Test edge cases like empty topic and multiple unsubscribes.
		topic := New[string]()

		// Test Recent on empty topic.
		if _, ok := Recent(topic); ok {
			t.Error("Recent on empty topic should return false")
		}

		// Test multiple Unsubscribe calls.
		receiver, _, err := topic.Subscribe(0, false /* includeRecent */)
		if err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}
		receiver.Unsubscribe()
		receiver.Unsubscribe() // Should be idempotent, no panic or error.

		// Test Close idempotence.
		if err := topic.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
		if err := topic.Close(); err != nil {
			t.Fatalf("Second Close failed: %v", err)
		}
	})

	t.Run("Recent", func(t *testing.T) {
		// Test edge cases like empty topic and multiple unsubscribes.
		topic := New[int]()

		// Test Recent on empty topic.
		if _, ok := Recent(topic); ok {
			t.Error("Recent on empty topic should return false")
		}

		topic.SendCh() <- 10

		if v, ok := Recent(topic); !ok {
			t.Error("Recent must return a value")
		} else if v != 10 {
			t.Errorf("Recent must've returned 10, got %d", v)
		}
	})

	t.Run("IncludeRecent", func(t *testing.T) {
		// Test limit > 0 (most recent N messages).
		topic := New[int]()

		// Send more messages than buffer size.
		for i := 1; i <= 4; i++ {
			topic.Send(i)
		}

		receiver, ch, err := topic.Subscribe(0, true /* includeRecent */)
		if err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		// Send one more message.
		topic.Send(5)

		// Receive messages (should get only the most recent 2: 3 and 4).
		msgs := []int{}
		for i := 0; i < 2; i++ {
			select {
			case msg := <-ch:
				msgs = append(msgs, msg)
			case <-time.After(time.Second):
				t.Fatal("Timed out waiting for message")
			}
		}

		// Verify only recent messages are received.
		expected := []int{4, 5}
		for i, msg := range msgs {
			if msg != expected[i] {
				t.Errorf("Expected message %d, got %d", expected[i], msg)
			}
		}

		receiver.Unsubscribe()
	})
}
