package valkey

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
	"unsafe"

	outbox "github.com/gopherex/pg-outbox"
	valkeygo "github.com/valkey-io/valkey-go"
)

type fakeClient struct {
	commands [][]string
}

func (f *fakeClient) B() valkeygo.Builder {
	var b valkeygo.Builder
	// valkey-go initializes real client builders with internal/cmds.NoSlot.
	// The constructor is internal, so tests set the same flag on the alias.
	reflect.NewAt(reflect.TypeOf(b).Field(0).Type, unsafe.Pointer(reflect.ValueOf(&b).Elem().Field(0).UnsafeAddr())).
		Elem().
		SetUint(1 << 15)
	return b
}

func (f *fakeClient) Do(_ context.Context, cmd valkeygo.Completed) valkeygo.ValkeyResult {
	f.commands = append(f.commands, cmd.Commands())
	return valkeygo.ValkeyResult{}
}

func TestPubSubPublish(t *testing.T) {
	client := &fakeClient{}
	pub := NewPubSub(client)

	err := pub.Publish(context.Background(), []outbox.Message{{
		Topic:   "orders",
		Payload: []byte("payload"),
	}})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	want := [][]string{{"PUBLISH", "orders", "payload"}}
	if !reflect.DeepEqual(client.commands, want) {
		t.Fatalf("commands = %#v, want %#v", client.commands, want)
	}
}

func TestPubSubPublishReturnsMapperError(t *testing.T) {
	want := errors.New("map")
	pub := NewPubSub(&fakeClient{}, WithPubSubMapper(func(outbox.Message) (string, string, error) {
		return "", "", want
	}))
	err := pub.Publish(context.Background(), []outbox.Message{{Topic: "orders"}})
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

func TestStreamPublishMapsMessages(t *testing.T) {
	client := &fakeClient{}
	pub := NewStream(client)
	createdAt := time.Unix(123, 456)

	err := pub.Publish(context.Background(), []outbox.Message{{
		ID:           "id-1",
		Topic:        "orders",
		PartitionKey: "order-1",
		Payload:      []byte("payload"),
		ContentType:  "application/json",
		MessageType:  "OrderCreated",
		Headers:      map[string]string{"trace-id": "trace-1"},
		CreatedAt:    createdAt,
	}})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(client.commands) != 1 {
		t.Fatalf("commands = %d, want 1", len(client.commands))
	}
	cmd := client.commands[0]
	if !reflect.DeepEqual(cmd[:3], []string{"XADD", "orders", "*"}) {
		t.Fatalf("command prefix = %#v", cmd[:3])
	}
	assertField(t, cmd, StreamFieldPayload, "payload")
	assertField(t, cmd, StreamFieldOutboxID, "id-1")
	assertField(t, cmd, StreamFieldPartitionKey, "order-1")
	assertField(t, cmd, StreamFieldContentType, "application/json")
	assertField(t, cmd, StreamFieldMessageType, "OrderCreated")
	assertField(t, cmd, streamHeaderPrefix+"trace-id", "trace-1")
	assertField(t, cmd, StreamFieldCreatedAtUnixNano, "123000000456")
}

func TestStreamPublishReturnsMapperError(t *testing.T) {
	want := errors.New("map")
	pub := NewStream(&fakeClient{}, WithStreamMapper(func(outbox.Message) (string, []StreamField, error) {
		return "", nil, want
	}))
	err := pub.Publish(context.Background(), []outbox.Message{{Topic: "orders"}})
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

func assertField(t *testing.T, cmd []string, name, want string) {
	t.Helper()
	for i := 3; i+1 < len(cmd); i += 2 {
		if cmd[i] == name {
			if cmd[i+1] != want {
				t.Fatalf("%s = %q, want %q", name, cmd[i+1], want)
			}
			return
		}
	}
	t.Fatalf("missing field %s in %#v", name, cmd)
}
