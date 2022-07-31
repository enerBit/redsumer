package client

import (
	"context"
	"fmt"
	"log"

	"github.com/go-redis/redis/v9"
)

func RedisClient(RedisHost string, RedisPort, Db int) *redis.Client {
	log.Printf("%c Client started", 129395)

	redisAdress := fmt.Sprintf("%s:%d", RedisHost, RedisPort)

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAdress,
		DB:   Db,
	})

	ctx := context.Background()

	_, err := redisClient.Ping(ctx).Result()

	if err != nil {
		log.Fatal("Unable to connect to ", redisAdress)
	}

	log.Println("Connected to Redis")

	return redisClient
}
