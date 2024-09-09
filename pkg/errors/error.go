package errors_custom

import "errors"

var (
	ErrStreamNotFound  = errors.New("stream key not found")
	ErrKeyNotFound  = errors.New("key not found")
	ErrGroupNotCreated = errors.New("group not created")
	ErrNoAckedMessage = errors.New("no acked message")
)
