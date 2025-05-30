// Copyright (c) 2025 Visvasity LLC

// Package topic implements a generic, buffering publish-subscribe messaging
// system with dynamic fanout.
//
// Messages sent to a Topic are duplicated and delivered to all subscribed
// receivers. Incoming messages are queued in-memory when receivers are not
// ready, with configurable buffering behavior per receiver. Receivers can be
// added or removed dynamically, and the most recent message can be queried.
//
// The Topic type is safe for concurrent use by multiple goroutines.
package topic

import (
	"context"
	"os"
	"reflect"
	"slices"
	"sync"
)

type anyReceiver[T any] interface {
	Close()
	add(T)
}

// Topic represents a publish-subscribe channel that duplicates messages to all
// subscribed receivers. Messages are queued in-memory for slow receivers, and
// the buffering behavior is configured per receiver via Subscribe. Topics are
// created with New and support generic message types.
type Topic[T any] struct {
	// lifeCtx and lifeCancel manage topic lifecycle and cancellation.
	lifeCtx    context.Context
	lifeCancel context.CancelCauseFunc

	// wg tracks internal goroutines.
	wg sync.WaitGroup

	// mu synchronizes access to receivers and closed state.
	mu sync.Mutex

	// receivers is the list of all receivers for the topic.
	receivers []anyReceiver[T]

	// recentValue holds the latest value sent to the topic.
	recentValue T

	// numValues holds total number of values sent over the topic.
	numValues int64

	// sendCh is an optional channel backed by a goroutine to receiver values
	// over a channel instead.
	sendCh chan T
}

// New creates a new Topic for messages of type T. The returned Topic is ready
// to accept messages via Send and subscribers via Subscribe. The provided
// context controls the topic's lifecycle; when it cancels, the topic closes.
//
// Example:
//
//	topic := topic.New[string]()
//	topic.Send("Hello, world!")
func New[T any]() *Topic[T] {
	ctx, cancel := context.WithCancelCause(context.Background())
	t := &Topic[T]{
		lifeCtx:    ctx,
		lifeCancel: cancel,
	}
	return t
}

// Close shuts down the Topic, preventing further subscriptions or message sends.
// After closing, Send is a no-op, Subscribe returns an error, and Recent may
// return false. Close is idempotent.
func (t *Topic[T]) Close() error {
	defer t.wg.Wait()

	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.isClosed(); err != nil {
		return err
	}

	t.lifeCancel(os.ErrClosed)
	receivers := t.receivers
	t.receivers = nil

	t.mu.Unlock()
	defer t.mu.Lock()

	for _, r := range receivers {
		r.Close()
	}
	return nil
}

// isClosed returns non-nil error if topic is closed.
func (t *Topic[T]) isClosed() error {
	return context.Cause(t.lifeCtx)
}

// removeUnlocked removes input receiver by it's reflect.Value. Returns false
// if receiver is not found.
func (t *Topic[T]) removeUnlocked(r any) bool {
	rvalue := reflect.ValueOf(r)

	t.mu.Lock()
	defer t.mu.Unlock()

	index := slices.IndexFunc(t.receivers, func(v anyReceiver[T]) bool {
		return reflect.ValueOf(v) == rvalue
	})
	if index == -1 {
		return false
	}

	t.receivers = slices.Delete(t.receivers, index, index+1)
	return true
}

func (t *Topic[T]) goRun(f func(context.Context)) {
	t.wg.Add(1)
	go func() {
		f(t.lifeCtx)
		t.wg.Done()
	}()
}

// Last returns the most recent message sent over the Topic.  Returns false if
// no messages are ever sent over the topic.
func (t *Topic[T]) Last() (v T, ok bool) {
	if err := t.isClosed(); err != nil {
		return v, false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.numValues == 0 {
		return v, false
	}
	return t.recentValue, true
}

// Send publishes a message to the Topic. The message is duplicated and
// delivered to all subscribed receivers. Returns os.ErrClosed if the Topic is
// closed.
func (t *Topic[T]) Send(v T) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.isClosed(); err != nil {
		return err
	}

	for _, r := range t.receivers {
		r.add(v)
	}

	t.numValues++
	t.recentValue = v
	return nil
}
