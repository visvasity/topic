// Copyright (c) 2023 BVK Chaitanya

package topic

import (
	"context"
	"os"
)

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

// ReceiveCh returns a select-friendly channel to receive messages from a
// topic's receiver. Inter-mixing Receive/All methods with receiver-channels
// will result in out-of-order delivery of values, but values are delivered
// only once. Also, using receive channels is more expensive than direct
// receive methods.
//
// NOTE: A background helper goroutine is used to retrieve messages from the
// topic and publish them to the topic. At max one background goroutine is
// created on demand and it is private to the input receiver.
//
// Returned channel will be closed if the topic is closed or receiver is
// unsubscribed.
func ReceiveCh[T any](r *Receiver[T]) (<-chan T, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.isClosed() || r.topic.isClosed() {
		return nil, os.ErrClosed
	}

	if r.receiveCh == nil {
		r.receiveCh = make(chan T)

		r.topic.wg.Add(1)
		go func() {
			defer r.topic.wg.Done()
			defer close(r.receiveCh)

			for !r.isClosed() {
				v, err := r.doReceive(r.lifeCtx)
				if err != nil {
					return
				}

				select {
				case <-r.lifeCtx.Done():
					return
				case r.receiveCh <- v:
				}
			}
		}()
	}

	return r.receiveCh, nil
}
