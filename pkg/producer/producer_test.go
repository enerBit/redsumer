package producer

import (
	"context"
	"testing"

	"github.com/enerBit/redsumer/pkg/client"
	"github.com/go-redis/redismock/v9"
	"github.com/redis/go-redis/v9"
)

func TestProduceSuccess(t *testing.T) {
	db, mock := redismock.NewClientMock()
	defer db.Close()
	producer := Producer{
		Client: db,
		RedisArgs: client.RedisArgs{
			RedisHost: "localhost",
			RedisPort: 6379,
			Db:        0,
		},
		ProducerArgs: ProducerArgs{
			StreamName: "testStream",
		},
	}
	message := map[string]interface{}{
		"key": "value",
	}
	mock.ExpectXAdd(&redis.XAddArgs{Stream: producer.ProducerArgs.StreamName, MaxLen: 0, Values: message}).SetVal("1")

	ctx := context.Background()
	err := producer.Produce(ctx, message)
	if err != nil {
		t.Error(err)
	}
}
func TestProduceError(t *testing.T) {
	db, mock := redismock.NewClientMock()
	defer db.Close()
	producer := Producer{
		Client: db,
		RedisArgs: client.RedisArgs{
			RedisHost: "localhost",
			RedisPort: 6379,
			Db:        0,
		},
		ProducerArgs: ProducerArgs{
			StreamName: "testStream",
		},
	}
	message := map[string]interface{}{
		"key": "value",
	}
	mock.ExpectXAdd(&redis.XAddArgs{Stream: producer.ProducerArgs.StreamName, MaxLen: 0, Values: message}).RedisNil()

	ctx := context.Background()
	err := producer.Produce(ctx, message)
	if err == nil {
		t.Error("Expected error, got nil")
	}
	if err != redis.Nil {
		t.Error(err)
	}
}
