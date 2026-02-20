package bus

import (
	eventbus "github.com/asaskevich/EventBus"
)

type Subscriber interface {
	Subscribe(topic string, fn any) error
	Unsubscribe(topic string, handler any) error
}

type Publisher interface {
	Publish(topic string, args ...any)
}

type Bus interface {
	Subscriber
	Publisher
}

func New() Bus {
	return &EventBus{eventbus.New()}
}

type EventBus struct {
	bus eventbus.Bus
}

func (e *EventBus) Publish(topic string, args ...any) {
	e.bus.Publish(topic, args...)
}

func (e *EventBus) Subscribe(topic string, handler any) error {
	return e.bus.Subscribe(topic, handler)
}

func (e *EventBus) Unsubscribe(topic string, handler any) error {
	return e.bus.Unsubscribe(topic, handler)
}

type NoopBus struct{}

func (b *NoopBus) Publish(topic string, args ...any)           {}
func (b *NoopBus) Subscribe(topic string, handler any) error   { return nil }
func (b *NoopBus) Unsubscribe(topic string, handler any) error { return nil }
