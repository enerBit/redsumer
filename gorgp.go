package gorgp

import (
	"github.com/enerBit/redsumer/pkg/client"
	"github.com/enerBit/redsumer/pkg/consumer"
	"github.com/enerBit/redsumer/pkg/producer"
	"github.com/go-redis/redis/v9"
)

type RedisClient struct {
	GroupName, ConsumerName, StreamName, RedisHost string
	RedisPort, Db                                  int
}

type NewClient interface {
	getClient(RedisHost string, RedisPort, Db int) *redis.Client
}

func (r RedisClient) getClient() *redis.Client {
	client := client.RedisClient(r.RedisHost, r.RedisPort, r.Db)
	return client
}

func (r RedisClient) Consume() []redis.XMessage {
	client := r.getClient()
	messages := consumer.Consume(client, r.GroupName, r.ConsumerName, r.StreamName)
	client.Close()
	return messages
}

func (r RedisClient) AcknowledgeMessage(id string) {
	client := r.getClient()
	consumer.AcknowledgeMessage(client, r.GroupName, r.StreamName, id)
	client.Close()
}

func (r RedisClient) Produce(message map[string]interface{}) {
	client := r.getClient()
	producer.Produce(client, r.StreamName, message)
	client.Close()
}
