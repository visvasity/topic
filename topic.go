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
	"errors"
	"iter"
	"os"
	"slices"
	"sync"
)

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
	receivers []*Receiver[T]

	// recentValue holds the latest value sent to the topic.
	recentValue T

	// numValues holds total number of values sent over the topic.
	numValues int64

	// sendCh is an optional channel backed by a goroutine to receiver values
	// over a channel instead.
	sendCh chan T
}

// Receiver represents a subscriber to a Topic. It provides methods to manage
// subscription lifecycle, such as unsubscribing, and to receive messages.
type Receiver[T any] struct {
	// lifeCtx and lifeCancel manage topic lifecycle and cancellation.
	lifeCtx    context.Context
	lifeCancel context.CancelCauseFunc

	// topic holds reference to the topic.
	topic *Topic[T]

	// mu synchronizes access to queue and closed state.
	mu sync.Mutex

	// cond signals receivers when new messages are available or state changes.
	cond sync.Cond

	// queue holds zero or more incoming messages not yet received.
	queue []T

	// limit indicates maximum number of messages to buffer in the queue.
	// 0: unbounded; +N: newest N values; -N: oldest |N| values.
	limit int

	// receiveCh when non-nil indicates retrieves values ONLY over this channel.
	receiveCh chan T
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

	if t.isClosed() {
		return nil
	}

	t.lifeCancel(os.ErrClosed)
	for _, r := range t.receivers {
		r.close()
	}

	t.receivers = nil
	return nil
}

// isClosed returns true if topic is closed.
func (t *Topic[T]) isClosed() bool {
	return t.lifeCtx.Err() != nil
}

// Last returns the most recent message sent over the Topic.  Returns false if
// no messages are ever sent over the topic.
func (t *Topic[T]) Last() (v T, ok bool) {
	if t.isClosed() {
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

	if t.isClosed() {
		return context.Cause(t.lifeCtx)
	}

	for _, r := range t.receivers {
		r.add(v)
	}

	t.numValues++
	t.recentValue = v
	return nil
}

// Subscribe adds a new receiver to the Topic, returning a Receiver for consuming
// messages via Receive. The limit parameter controls the receiver's queue behavior:
//
//   - limit == 0: Unbounded queue, buffering all messages (memory-limited).
//   - limit > 0: Buffers the most recent limit messages, discarding older ones.
//   - limit < 0: Buffers the oldest limit messages, discarding newer ones.
//
// If the Topic is closed or its context is canceled, Subscribe returns an error.
func Subscribe[T any](t *Topic[T], limit int, includeLast bool) (*Receiver[T], error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.isClosed() {
		return nil, context.Cause(t.lifeCtx)
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	r := &Receiver[T]{
		lifeCtx:    ctx,
		lifeCancel: cancel,
		topic:      t,
		limit:      limit,
	}
	r.cond.L = &r.mu

	t.receivers = append(t.receivers, r)
	if includeLast && t.numValues > 0 {
		r.add(t.recentValue)
	}
	return r, nil
}

// isClosed returns true if receiver is closed or unsubscribed.
func (r *Receiver[T]) isClosed() bool {
	return r.lifeCtx.Err() != nil
}

// Unsubscribe removes the receiver from the Topic, discarding pending messages.
// Unsubscribe is idempotent. After unsubscribing, the receiver cannot be reused.
func (r *Receiver[T]) Unsubscribe() {
	r.topic.mu.Lock()
	if i := slices.Index(r.topic.receivers, r); i >= 0 {
		r.topic.receivers = slices.Delete(r.topic.receivers, i, i+1)
	}
	r.topic.mu.Unlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.isClosed() {
		return
	}

	r.lifeCancel(os.ErrClosed)
	r.queue = nil

	r.cond.Broadcast()
}

// Receive returns the next available message from the receiver's queue, blocking
// until a message is available or the topic/receiver is closed. If closed, it
// returns [os.ErrClosed]. If the topic's context is canceled, it returns the context's
// error.
func (r *Receiver[T]) Receive() (T, error) {
	return r.doReceive(context.Background())
}

func (r *Receiver[T]) doReceive(ctx context.Context) (T, error) {
	var zero T

	r.mu.Lock()
	defer r.mu.Unlock()

	for ctx.Err() == nil {
		if r.isClosed() {
			return zero, os.ErrClosed
		}

		if r.topic.isClosed() {
			return zero, context.Cause(r.topic.lifeCtx)
		}

		if len(r.queue) > 0 {
			v := r.queue[0]
			r.queue = slices.Delete(r.queue, 0, 1)
			return v, nil
		}

		r.cond.Wait()
	}

	return zero, context.Cause(ctx)
}

// add appends a message to the receiver's queue, respecting the limit.
func (r *Receiver[T]) add(v T) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.isClosed() {
		return
	}

	defer r.cond.Broadcast()

	if r.limit == 0 {
		r.queue = append(r.queue, v)
		return
	}

	if r.limit > 0 {
		if len(r.queue) < r.limit {
			r.queue = append(r.queue, v)
			return
		}
		r.queue = slices.Delete(r.queue, 0, 1)
		r.queue = append(r.queue, v)
	}

	if r.limit < 0 {
		if len(r.queue) < -r.limit {
			r.queue = append(r.queue, v)
			return
		}
	}
}

// close marks the receiver as closed and clears its queue.
func (r *Receiver[T]) close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.isClosed() {
		return
	}

	r.lifeCancel(os.ErrClosed)
	r.queue = nil

	r.cond.Broadcast()
}

// All returns an iterator to process all incoming values over the
// topic. Iterator stops with nil if the topic or the receiver is closed.
func (r *Receiver[T]) All(ctx context.Context, errp *error) iter.Seq[T] {
	return func(yield func(T) bool) {
		stopf := context.AfterFunc(ctx, func() {
			r.mu.Lock()
			r.cond.Broadcast()
			r.mu.Unlock()
		})
		defer stopf()

		for {
			v, err := r.doReceive(ctx)
			if err != nil {
				if !errors.Is(err, os.ErrClosed) {
					*errp = err
				}
				return
			}
			if !yield(v) {
				return
			}
		}
	}
}
