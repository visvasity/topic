// Copyright (c) 2023 BVK Chaitanya

package topic

import "context"

// Recent returns the most recent message sent to the Topic and a boolean
// indicating whether a message exists. If no messages have been sent or the
// Topic is closed, it returns false.
//
// Example:
//
//	topic := topic.New[int](context.Background())
//	topic.Send(42)
//	if v, ok := Recent(topic); ok {
//	    fmt.Println(v) // Prints 42
//	}
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
// topic/receiver is closed, the topic's context error if its context is
// canceled, or the context's error if the provided context is canceled.
//
// NOTE: This function is more expensive than the non-context aware Receive
// method because [context.AfterFunc] setup is required to wake up the internal
// condition variable when the input context is canceled/expired.
//
// Example:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	defer cancel()
//	receiver, _ := topic.Subscribe(5, false)
//	msg, err := Receive(ctx, receiver)
//	if err != nil { /* handle error */ }
//	fmt.Println(msg)
func Receive[T any](ctx context.Context, r *Receiver[T]) (T, error) {
	stopf := context.AfterFunc(ctx, func() {
		r.mu.Lock()
		r.cond.Broadcast()
		r.mu.Unlock()
	})
	defer stopf()

	return r.doReceive(ctx)
}

// SendCh returns a channel to send messages over to a topic. This is an
// alternative to the Send method, where a channel is more appropriate.
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
