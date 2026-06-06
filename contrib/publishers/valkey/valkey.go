// Package valkey publishes outbox messages to Valkey Pub/Sub channels or
// Streams with github.com/valkey-io/valkey-go.
package valkey

import (
	"context"
	"strconv"

	outbox "github.com/gopherex/pg-outbox"
	valkeygo "github.com/valkey-io/valkey-go"
)

const (
	// StreamFieldPayload stores Message.Payload.
	StreamFieldPayload = "payload"
	// StreamFieldOutboxID stores Message.ID when it is set.
	StreamFieldOutboxID = "pg_outbox_id"
	// StreamFieldPartitionKey stores Message.PartitionKey when it is set.
	StreamFieldPartitionKey = "partition_key"
	// StreamFieldContentType stores Message.ContentType when it is set.
	StreamFieldContentType = "content_type"
	// StreamFieldMessageType stores Message.MessageType when it is set.
	StreamFieldMessageType = "message_type"
	// StreamFieldCreatedAtUnixNano stores Message.CreatedAt as Unix nanoseconds
	// when CreatedAt is populated.
	StreamFieldCreatedAtUnixNano = "created_at_unix_nano"
)

const streamHeaderPrefix = "header."

// Client is the subset of valkey.Client used by the publishers.
type Client interface {
	B() valkeygo.Builder
	Do(ctx context.Context, cmd valkeygo.Completed) valkeygo.ValkeyResult
}

// StreamField is one field/value pair for XADD.
type StreamField struct {
	Name  string
	Value string
}

// PubSubMapper converts an outbox message to a Valkey Pub/Sub channel and
// message. Valkey bulk strings are binary-safe; the default mapper converts
// Message.Payload to string without changing the bytes.
type PubSubMapper func(outbox.Message) (channel string, message string, err error)

// StreamMapper converts an outbox message to a Valkey Stream key and XADD
// fields. The publisher uses "*" as the stream entry id.
type StreamMapper func(outbox.Message) (stream string, fields []StreamField, err error)

// PubSubOption configures PubSubPublisher.
type PubSubOption func(*PubSubPublisher)

// StreamOption configures StreamPublisher.
type StreamOption func(*StreamPublisher)

// WithPubSubMapper replaces the default Pub/Sub mapping.
func WithPubSubMapper(mapper PubSubMapper) PubSubOption {
	return func(p *PubSubPublisher) {
		if mapper != nil {
			p.mapper = mapper
		}
	}
}

// WithStreamMapper replaces the default stream mapping.
func WithStreamMapper(mapper StreamMapper) StreamOption {
	return func(p *StreamPublisher) {
		if mapper != nil {
			p.mapper = mapper
		}
	}
}

// PubSubPublisher delivers outbox batches to Valkey Pub/Sub channels.
type PubSubPublisher struct {
	client Client
	mapper PubSubMapper
}

// NewPubSub builds a Valkey Pub/Sub publisher. Message.Topic is used as the
// channel by default.
func NewPubSub(client Client, opts ...PubSubOption) *PubSubPublisher {
	p := &PubSubPublisher{client: client, mapper: DefaultPubSubMapper}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Publish publishes every message to Message.Topic as a Valkey Pub/Sub channel.
func (p *PubSubPublisher) Publish(ctx context.Context, msgs []outbox.Message) error {
	for _, m := range msgs {
		channel, payload, err := p.mapper(m)
		if err != nil {
			return err
		}
		cmd := p.client.B().Publish().Channel(channel).Message(payload).Build()
		if err := p.client.Do(ctx, cmd).Error(); err != nil {
			return err
		}
	}
	return nil
}

// StreamPublisher delivers outbox batches to Valkey Streams.
type StreamPublisher struct {
	client Client
	mapper StreamMapper
}

// NewStream builds a Valkey Streams publisher. Message.Topic is used as the
// stream key by default.
func NewStream(client Client, opts ...StreamOption) *StreamPublisher {
	p := &StreamPublisher{client: client, mapper: DefaultStreamMapper}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Publish XADDs every message to Message.Topic as a Valkey stream key.
func (p *StreamPublisher) Publish(ctx context.Context, msgs []outbox.Message) error {
	for _, m := range msgs {
		stream, fields, err := p.mapper(m)
		if err != nil {
			return err
		}
		fv := p.client.B().Xadd().Key(stream).Id("*").FieldValue()
		for _, field := range fields {
			fv = fv.FieldValue(field.Name, field.Value)
		}
		if err := p.client.Do(ctx, fv.Build()).Error(); err != nil {
			return err
		}
	}
	return nil
}

// DefaultPubSubMapper publishes Message.Payload as the Pub/Sub message and
// Message.Topic as the channel.
func DefaultPubSubMapper(m outbox.Message) (string, string, error) {
	return m.Topic, string(m.Payload), nil
}

// DefaultStreamMapper maps Topic to the stream key and stores payload,
// first-class outbox metadata, and Headers as stream fields.
func DefaultStreamMapper(m outbox.Message) (string, []StreamField, error) {
	fields := make([]StreamField, 0, len(m.Headers)+6)
	fields = append(fields, StreamField{Name: StreamFieldPayload, Value: string(m.Payload)})
	if m.ID != "" {
		fields = append(fields, StreamField{Name: StreamFieldOutboxID, Value: m.ID})
	}
	if m.PartitionKey != "" {
		fields = append(fields, StreamField{Name: StreamFieldPartitionKey, Value: m.PartitionKey})
	}
	if m.ContentType != "" {
		fields = append(fields, StreamField{Name: StreamFieldContentType, Value: m.ContentType})
	}
	if m.MessageType != "" {
		fields = append(fields, StreamField{Name: StreamFieldMessageType, Value: m.MessageType})
	}
	if !m.CreatedAt.IsZero() {
		fields = append(fields, StreamField{
			Name:  StreamFieldCreatedAtUnixNano,
			Value: strconv.FormatInt(m.CreatedAt.UnixNano(), 10),
		})
	}
	for k, v := range m.Headers {
		fields = append(fields, StreamField{Name: streamHeaderPrefix + k, Value: v})
	}
	return m.Topic, fields, nil
}
