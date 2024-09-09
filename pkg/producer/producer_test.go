package producer

import (
	"context"
	"testing"

	"github.com/enerBit/redsumer/v3/pkg/client"
	"github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
)

const (
	streamName   string = "stream-test"
	groupName    string = "group-test"
	consumerName string = "consumer-test"
)

func TestProduceSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	db := mock.NewClient(ctrl)

	message := map[string]string{
		"key": "value",
	}

	db.EXPECT().Do(ctx, mock.Match("XADD", streamName, "*", "key", "value")).Return(mock.ErrorResult(nil))
	clientArg := &client.ClientArgs{
		Instance: db,
	}
	p := Producer{
		Client: clientArg,
	}
	err := p.Produce(ctx, message, streamName)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}
func TestProduceError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	db := mock.NewClient(ctrl)

	message := map[string]string{
		"key": "value",
	}

	db.EXPECT().Do(ctx, mock.Match("XADD", streamName, "*", "key", "value")).Return(mock.Result(mock.ValkeyError("error")))
	clientArg := &client.ClientArgs{
		Instance: db,
	}
	p := Producer{
		Client: clientArg,
	}
	err := p.Produce(ctx, message, streamName)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}