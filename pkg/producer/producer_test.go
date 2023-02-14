package producer

import (
	"testing"

	"github.com/go-redis/redismock/v9"
	"github.com/redis/go-redis/v9"
)

func TestProducer(t *testing.T) {
	db, mock := redismock.NewClientMock()

	streamName := "test:stream"
	data := map[string]any{"algo": 10}

	mock.ExpectXAdd(&redis.XAddArgs{
		Stream: streamName,
		MaxLen: 0,
		Values: data,
	}).RedisNil()

	err := Produce(db, streamName, data)
	if err == nil {
		t.Error("wrong error")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
