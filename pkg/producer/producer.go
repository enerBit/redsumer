package producer

import (
	"context"

	"github.com/enerBit/redsumer/v3/pkg/client"
)

type Producer struct {
	Client *client.ClientArgs
}

// Produce sends a message to the specified stream.
// It takes a context and a message map as input.
// Returns an error if there was a problem sending the message.
func (p *Producer) Produce(ctx context.Context, message map[string]string, streamName string) error {
	cmd := p.Client.Instance.B().Xadd().Key(streamName).Id("*").FieldValue()
	for k, v := range message {
		cmd.FieldValue(k, v)
	}

	err := p.Client.Instance.Do(ctx, cmd.Build()).Error()
	if err != nil {
		return err
	}

	return nil
}
