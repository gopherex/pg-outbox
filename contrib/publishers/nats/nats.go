// Package nats publishes outbox messages to NATS subjects.
package nats

import (
	"context"

	outbox "github.com/gopherex/pg-outbox"
	natsgo "github.com/nats-io/nats.go"
)

const (
	// HeaderOutboxID stores the outbox row id on published messages.
	HeaderOutboxID = "pg-outbox-id"
	// HeaderContentType stores Message.ContentType when it is set.
	HeaderContentType = "content-type"
	// HeaderMessageType stores Message.MessageType when it is set.
	HeaderMessageType = "message-type"
	// HeaderPartitionKey stores Message.PartitionKey when it is set.
	HeaderPartitionKey = "partition-key"
)

// Client is the subset of *nats.Conn used by Publisher.
type Client interface {
	PublishMsg(msg *natsgo.Msg) error
	FlushWithContext(ctx context.Context) error
}

// Mapper converts an outbox message to a NATS message.
type Mapper func(outbox.Message) (*natsgo.Msg, error)

// Option configures Publisher.
type Option func(*Publisher)

// WithMapper replaces the default outbox-to-NATS mapping.
func WithMapper(mapper Mapper) Option {
	return func(p *Publisher) {
		if mapper != nil {
			p.mapper = mapper
		}
	}
}

// WithoutFlush disables FlushWithContext after each successfully queued batch.
func WithoutFlush() Option {
	return func(p *Publisher) { p.flush = false }
}

// Publisher delivers outbox batches to NATS.
type Publisher struct {
	client Client
	mapper Mapper
	flush  bool
}

// New builds a NATS publisher. A nil client will panic when Publish is called,
// matching the behavior of using a nil transport dependency directly.
func New(client Client, opts ...Option) *Publisher {
	p := &Publisher{
		client: client,
		mapper: DefaultMapper,
		flush:  true,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Publish publishes every message to Message.Topic as the NATS subject.
func (p *Publisher) Publish(ctx context.Context, msgs []outbox.Message) error {
	for _, m := range msgs {
		msg, err := p.mapper(m)
		if err != nil {
			return err
		}
		if err := p.client.PublishMsg(msg); err != nil {
			return err
		}
	}
	if p.flush {
		return p.client.FlushWithContext(ctx)
	}
	return nil
}

// DefaultMapper maps Topic to Subject, Payload to Data, and Headers to NATS
// headers. Outbox metadata is added as reserved headers.
func DefaultMapper(m outbox.Message) (*natsgo.Msg, error) {
	msg := &natsgo.Msg{
		Subject: m.Topic,
		Data:    m.Payload,
		Header:  make(natsgo.Header, len(m.Headers)+4),
	}
	for k, v := range m.Headers {
		msg.Header.Set(k, v)
	}
	setMetadata(msg.Header, m)
	return msg, nil
}

func setMetadata(h natsgo.Header, m outbox.Message) {
	if m.ID != "" {
		h.Set(HeaderOutboxID, m.ID)
	}
	if m.ContentType != "" {
		h.Set(HeaderContentType, m.ContentType)
	}
	if m.MessageType != "" {
		h.Set(HeaderMessageType, m.MessageType)
	}
	if m.PartitionKey != "" {
		h.Set(HeaderPartitionKey, m.PartitionKey)
	}
}
