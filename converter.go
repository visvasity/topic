// Copyright (c) 2025 Visvasity LLC

package topic

import "context"

type converter[T any, U any] struct {
	receiver Receiver[U]
	convert  func(T) U
}

func (c *converter[T, U]) Close() {
	c.receiver.Close()
}

func (c *converter[T, U]) add(v T) {
	c.receiver.add(c.convert(v))
}

// SubscribeFunc is similar to Subscribe, but converts the received value from
// one type to another using the input helper function. Converter function is
// invoked under the topic's Send context, so it SHOULD NOT block for optimal
// performance.
func SubscribeFunc[T, U any](t *Topic[T], fn func(T) U, limit int, includeLast bool) (*Receiver[U], error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.isClosed(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	c := &converter[T, U]{
		convert: fn,
		receiver: Receiver[U]{
			lifeCtx:    ctx,
			lifeCancel: cancel,
			topic:      t,
			limit:      limit,
		},
	}
	c.receiver.cond.L = &c.receiver.mu

	t.receivers = append(t.receivers, c)
	if includeLast && t.numValues > 0 {
		c.receiver.add(c.convert(t.recentValue))
	}
	return &c.receiver, nil
}
