package client

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type RedisArgs struct {
	RedisHost string
	RedisPort int
	Db        int
}

// newRedisClient creates a new Redis client and returns it along with any error encountered.
// It takes a context.Context as input and uses the RedisArgs receiver to access the RedisHost, RedisPort, and Db fields.
// The Redis client is created with the specified Redis address and database number.
// It then sends a PING command to the Redis server to check the connection.
// The function returns the Redis client and any error encountered during the PING command.
func (r RedisArgs) NewRedisClient(ctx context.Context) (*redis.Client, error) {
	redisAdress := fmt.Sprintf("%s:%d", r.RedisHost, r.RedisPort)

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAdress,
		DB:   r.Db,
	})

	_, err := redisClient.Ping(ctx).Result()

	return redisClient, err
}
