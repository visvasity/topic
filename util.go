// Copyright (c) 2023 BVK Chaitanya

package topic

import (
	"context"
)

// Recent returns the most recent message sent to the Topic and a boolean
// indicating whether a message exists. If no messages have been sent or the
// Topic is closed, it returns false.
func Recent[T any](t *Topic[T]) (v T, ok bool) {
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

// Receive returns the next available message from the receiver's queue,
// blocking until a message is available, the topic/receiver is closed, or the
// provided context is canceled. It returns [os.ErrClosed] if the
// topic/receiver is closed, or the context's error if the provided context is
// canceled.
//
// NOTE: This function is more expensive than the non-context aware Receive
// method because [context.AfterFunc] setup is required to wake up the internal
// condition variable when the input context is canceled/expired.
func Receive[T any](ctx context.Context, r *Receiver[T]) (T, error) {
	stopf := context.AfterFunc(ctx, func() {
		r.mu.Lock()
		r.cond.Broadcast()
		r.mu.Unlock()
	})
	defer stopf()

	return r.doReceive(ctx, true /* failOnReceiveCh */)
}

// SendCh returns a select-friendly channel to send messages over to a
// topic. This is an alternative to the Send method, where a channel is more
// appropriate.
//
// NOTE: A background helper goroutine is used to receive messages over the
// channel and publish them to the topic. At max one background goroutine is
// created on demand and it is private to the input topic.
//
// Returned channel will block forever after the topic is closed.
func SendCh[T any](t *Topic[T]) (chan<- T, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.isClosed() {
		return nil, context.Cause(t.lifeCtx)
	}

	if t.sendCh == nil {
		t.sendCh = make(chan T)

		t.wg.Add(1)
		go func() {
			defer t.wg.Done()

			for {
				select {
				case <-t.lifeCtx.Done():
					return
				case v := <-t.sendCh:
					t.Send(v)
				}
			}
		}()
	}

	return t.sendCh, nil
}
