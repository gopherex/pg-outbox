// Package kafka publishes outbox messages with github.com/segmentio/kafka-go.
package kafka

import (
	"context"

	outbox "github.com/gopherex/pg-outbox"
	kafkago "github.com/segmentio/kafka-go"
)

const (
	// HeaderOutboxID stores the outbox row id on published messages.
	HeaderOutboxID = "pg-outbox-id"
	// HeaderContentType stores Message.ContentType when it is set.
	HeaderContentType = "content-type"
	// HeaderMessageType stores Message.MessageType when it is set.
	HeaderMessageType = "message-type"
)

// Writer is the subset of kafka-go's *kafka.Writer used by Publisher.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
}

// Mapper converts an outbox message to a Kafka message.
type Mapper func(outbox.Message) (kafkago.Message, error)

// Option configures Publisher.
type Option func(*Publisher)

// WithMapper replaces the default outbox-to-Kafka mapping.
func WithMapper(mapper Mapper) Option {
	return func(p *Publisher) {
		if mapper != nil {
			p.mapper = mapper
		}
	}
}

// Publisher delivers outbox batches to Kafka.
type Publisher struct {
	writer Writer
	mapper Mapper
}

// New builds a Kafka publisher. The writer can be a *kafka.Writer.
func New(writer Writer, opts ...Option) *Publisher {
	p := &Publisher{writer: writer, mapper: DefaultMapper}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Publish writes the whole outbox batch with one kafka-go WriteMessages call.
func (p *Publisher) Publish(ctx context.Context, msgs []outbox.Message) error {
	kmsgs := make([]kafkago.Message, len(msgs))
	for i, m := range msgs {
		kmsg, err := p.mapper(m)
		if err != nil {
			return err
		}
		kmsgs[i] = kmsg
	}
	return p.writer.WriteMessages(ctx, kmsgs...)
}

// DefaultMapper maps Topic to Kafka topic, PartitionKey to key, Payload to
// value, and Headers to Kafka headers. Outbox metadata is added as reserved
// headers.
func DefaultMapper(m outbox.Message) (kafkago.Message, error) {
	headers := make([]kafkago.Header, 0, len(m.Headers)+3)
	for k, v := range m.Headers {
		headers = append(headers, kafkago.Header{Key: k, Value: []byte(v)})
	}
	headers = appendMetadata(headers, m)
	return kafkago.Message{
		Topic:   m.Topic,
		Key:     []byte(m.PartitionKey),
		Value:   m.Payload,
		Headers: headers,
	}, nil
}

func appendMetadata(headers []kafkago.Header, m outbox.Message) []kafkago.Header {
	if m.ID != "" {
		headers = append(headers, kafkago.Header{Key: HeaderOutboxID, Value: []byte(m.ID)})
	}
	if m.ContentType != "" {
		headers = append(headers, kafkago.Header{Key: HeaderContentType, Value: []byte(m.ContentType)})
	}
	if m.MessageType != "" {
		headers = append(headers, kafkago.Header{Key: HeaderMessageType, Value: []byte(m.MessageType)})
	}
	return headers
}
