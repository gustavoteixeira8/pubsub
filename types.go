package pubsub

type SubCallback func(msg *Message) error

type Message struct {
	Payload any
}
