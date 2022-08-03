# Redsumer
## Golang Redis consumer

Redsumer is a Golang package that allows you to connect to a Redis stream to receive and send messages, in addition to creating the consumer group.

## Initialization

```go:
RedisConsumer := gorgp.RedisClient{
  RedisHost: "RedisHost", 
  RedisPort: int(6379), 
  Db: int(0), 
  StreamName: "StreamName", 
  GroupName: "GroupName", 
  ConsumerName: "ConsumerName"
 }
```

## Getting messages
```go:
pendingMessages := RedisConsumer.Consume()
```

## Sending messages

```go:
message := map[string]interface{}{
  "key1": "value1",
  "key2": "value2",
  "key3": 123,
}
```

```go:
RedisConsumer.Produce(message)
```

## Acknowledge message

```go:
messageId := "1518951480106-0"
RedisConsumer.AcknowledgeMessage(messageId)
```


