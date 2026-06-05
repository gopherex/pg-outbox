package outbox

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// stubExec is a non-nil Executor for tests that never touch the database.
type stubExec struct{}

func (stubExec) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (stubExec) Query(context.Context, string, ...any) (pgx.Rows, error) { return nil, nil }
func (stubExec) QueryRow(context.Context, string, ...any) pgx.Row        { return nil }

func TestNewInvalidSchema(t *testing.T) {
	_, err := New(nil, nil, nil, WithSchema("bad-schema"))
	if !errors.Is(err, ErrInvalidSchema) {
		t.Fatalf("err = %v, want ErrInvalidSchema", err)
	}
}

func TestNewNoExecutor(t *testing.T) {
	_, err := New(nil, nil, nil)
	if !errors.Is(err, ErrNoExecutor) {
		t.Fatalf("err = %v, want ErrNoExecutor", err)
	}
}

func TestEnqueueValidates(t *testing.T) {
	o, err := New(stubExec{}, stubExec{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := o.Enqueue(context.Background(), Message{Payload: []byte("x")}); !errors.Is(err, ErrEmptyTopic) {
		t.Fatalf("err = %v, want ErrEmptyTopic", err)
	}
}

func TestEnqueueValueNoCodec(t *testing.T) {
	o, err := New(stubExec{}, stubExec{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := o.EnqueueValue(context.Background(), "t", "", "", struct{}{}); !errors.Is(err, ErrNoCodec) {
		t.Fatalf("err = %v, want ErrNoCodec", err)
	}
}
