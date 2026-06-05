package message

import (
	"errors"
	"testing"
)

func TestMessageValidate(t *testing.T) {
	tests := []struct {
		name string
		msg  Message
		want error
	}{
		{"ok", Message{Topic: "t", Payload: []byte("x")}, nil},
		{"empty payload allowed when non-nil", Message{Topic: "t", Payload: []byte{}}, nil},
		{"no topic", Message{Payload: []byte("x")}, ErrEmptyTopic},
		{"nil payload", Message{Topic: "t"}, ErrNilPayload},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.msg.Validate(); !errors.Is(err, tt.want) {
				t.Fatalf("Validate() = %v, want %v", err, tt.want)
			}
		})
	}
}
