package client

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func NewRedisClient(RedisHost string, RedisPort, Db int) (*redis.Client, error) {
	redisAdress := fmt.Sprintf("%s:%d", RedisHost, RedisPort)

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAdress,
		DB:   Db,
	})

	ctx := context.Background()

	_, err := redisClient.Ping(ctx).Result()

	return redisClient, err
}
