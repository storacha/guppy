package bus

import (
	eventbus "github.com/asaskevich/EventBus"
)

type Subscriber interface {
	Subscribe(topic string, fn interface{}) error
	Unsubscribe(topic string, handler interface{}) error
}

type Publisher interface {
	Publish(topic string, args ...interface{})
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

func (e *EventBus) Publish(topic string, args ...interface{}) {
	e.bus.Publish(topic, args...)
}

func (e *EventBus) Subscribe(topic string, handler interface{}) error {
	return e.bus.Subscribe(topic, handler)
}

func (e *EventBus) Unsubscribe(topic string, handler interface{}) error {
	return e.bus.Unsubscribe(topic, handler)
}

type NoopBus struct{}

func (b *NoopBus) Publish(topic string, args ...interface{})           {}
func (b *NoopBus) Subscribe(topic string, handler interface{}) error   { return nil }
func (b *NoopBus) Unsubscribe(topic string, handler interface{}) error { return nil }
