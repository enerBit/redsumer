package client

import (
	"context"
	"fmt"
	"testing"
)

func TestNewRedisClient(t *testing.T) {
	ctx := context.Background()

	// Create a RedisArgs instance with test values
	redisArgs := RedisArgs{
		RedisHost: "localhost",
		RedisPort: 6379,
		Db:        0,
	}

	// Call the NewRedisClient function
	redisClient, _ := redisArgs.NewRedisClient(ctx)
	clientTypeString := fmt.Sprint(redisClient)

	if clientTypeString != "Redis<localhost:6379 db:0>" {
		t.Error(clientTypeString)
	}
}
