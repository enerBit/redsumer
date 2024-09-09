package consumer

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/enerBit/redsumer/v3/pkg/client"
	errors_custom "github.com/enerBit/redsumer/v3/pkg/errors"
	"github.com/valkey-io/valkey-go"
)


const (
	consumer_NEVER_DELIVERED_TO_OTHER_CONSUMERS_SO_FAR = ">"
	consumer_INITIAL_STREAM_ID                         = "0-0"
	consumer_NOGROUP                                   = "NOGROUP No such key"
)

type Consumer struct {
	Client *client.ClientArgs

	Tries []int

	StreamName   string
	GroupName    string
	ConsumerName string

	BatchSizeNewMessage int64
	BatchSizePending    *int64
	BatchSizeAutoClaim  *int64

	IdleStillMine    int64
	MinIdleAutoClaim int64

	latestPendingMessageId string
	nextIdAutoClaim        string
}


// InitConsumer creates a new Consumer instance.
// If any error occurs during the process, it returns nil and the error.
// Otherwise, it returns the created Consumer instance and nil error.
func (c *Consumer) InitConsumer(ctx context.Context) error {
	err := c.Client.InitClient(ctx)
	if err != nil {
		return err
	}

	c.latestPendingMessageId = consumer_INITIAL_STREAM_ID
	c.nextIdAutoClaim = consumer_INITIAL_STREAM_ID

	err = c.initGroup(ctx)
	if err != nil {
		return err
	}

	return nil
}

// exist checks if a key exists in the Valkey client.
// It returns an error if the key does not exist.
func (c *Consumer) exist(ctx context.Context, key string) error {
	cmd := c.Client.Instance.B().Exists().Key(key).Build()
	e, err := c.Client.Instance.Do(ctx, cmd).ToInt64()
	if err != nil {
		return err
	}

	if e != 1 {
		return errors_custom.ErrKeyNotFound
	}

	return nil
}

// createGroup creates a consumer group for processing messages from a stream.
// It waits for the stream to be available and then creates the group using the provided arguments.
// If the group already exists, it returns without an error.
// If any error occurs during the process, it is returned.
func (c *Consumer) initGroup(ctx context.Context) error {
	err := c.waitForStream(ctx)
	if err != nil {
		return err
	}

	cmd := c.Client.Instance.B().XgroupCreate().Key(c.StreamName).Group(c.GroupName).Id(consumer_INITIAL_STREAM_ID).Build()
	err = c.Client.Instance.Do(ctx, cmd).Error()
	if err != nil {
		var errV *valkey.ValkeyError
		if errors.As(err, &errV) {
			if errV.IsBusyGroup() {
				return nil
			} else {
				return errV
			}
		}
		return err
	}

	return nil
}

// waitForStream waits for the stream to be ready by checking its existence in the Valkey client.
// It retries for the specified number of times with a delay between each attempt.
// If the stream is ready, it returns nil. Otherwise, it returns an error.
func (c *Consumer) waitForStream(ctx context.Context) error {
	for _, waitTime := range c.Tries {
		err := c.exist(ctx, c.StreamName)
		if err == nil {
			return nil
		}
		time.Sleep(time.Second * time.Duration(waitTime))
	}
	return errors_custom.ErrStreamNotFound
}

// StillMine checks if a message with the given messageId is still pending for consumption.
// It returns true if the message is still pending, otherwise false.
// If there is an error while checking, it returns the error.
func (c *Consumer) StillMine(ctx context.Context, messageID string) (bool, error) {
	var isMine bool

	cmd := c.Client.Instance.B().Xpending().Key(c.StreamName).Group(c.GroupName).Idle(c.IdleStillMine).Start(messageID).End(messageID).Count(1).Consumer(c.ConsumerName).Build()
	v, err := c.Client.Instance.Do(ctx, cmd).ToArray()
	if err != nil {
		return isMine, err
	}

	if len(v) != 0 {
		isMine = true
	}

	return isMine, nil
}

// Ack acknowledges a message with the given message ID in the consumer group.
// It returns an error if there was a problem acknowledging the message.
func (c *Consumer) AcknowledgeMessage(ctx context.Context, messageID string) error {
	cmd := c.Client.Instance.B().Xack().Key(c.StreamName).Group(c.GroupName).Id(messageID).Build()
	v, err := c.Client.Instance.Do(ctx, cmd).AsBool()
	if err != nil {
		return err
	}
	if !v {
		return errors_custom.ErrNoAckedMessage
	}
	return nil
}

// newMessages retrieves new messages from a Valkey stream for the consumer.
// It uses the AsXRead command to read messages from the specified stream,
// using the consumer group and name provided in the Consumer struct.
// The function returns a slice of Valkey.XRangeEntry, which contains the retrieved messages,
// and an error if any occurred during the retrieval process.
func (c *Consumer) NewMessages(ctx context.Context) ([]valkey.XRangeEntry, error) {
	cmd := c.Client.Instance.B().Xreadgroup().Group(c.GroupName, c.ConsumerName).Count(c.BatchSizeNewMessage).Streams().Key(c.StreamName).Id(consumer_NEVER_DELIVERED_TO_OTHER_CONSUMERS_SO_FAR).Build()
	v, err := c.Client.Instance.Do(ctx, cmd).AsXRead()
	if err != nil {
		var errV *valkey.ValkeyError
		if errors.As(err, &errV) {
			if !errV.IsNil() {
				return nil, errV
			}
		} else {
			return nil, err
		}
	}

	fields := v[c.StreamName]
	return fields, nil
}

// pendingMessages retrieves pending messages from a Valkey stream.
// It returns a slice of Valkey.XRangeEntry representing the pending messages and an error if any.
func (c *Consumer) PendingMessages(ctx context.Context) ([]valkey.XRangeEntry, error) {
	cmd := c.Client.Instance.B().Xreadgroup().Group(c.GroupName, c.ConsumerName).Count(*c.BatchSizePending).Streams().Key(c.StreamName).Id(c.latestPendingMessageId).Build()
	v, err := c.Client.Instance.Do(ctx, cmd).AsXRead()
	if err != nil {
		var errV *valkey.ValkeyError
		if errors.As(err, &errV) {
			if !errV.IsNil() {
				return nil, errV
			}
		} else {
			return nil, err
		}
	}

	fields := v[c.StreamName]
	if len(fields) != 0 {
		c.latestPendingMessageId = fields[len(fields)-1].ID
	} else {
		c.latestPendingMessageId = consumer_INITIAL_STREAM_ID
	}
	return fields, nil
}

// claimedMessages returns a slice of claimed messages from the Valkey stream.
// It uses the XAutoClaim method of the Valkey client to automatically claim messages
// from the specified stream and group. The minimum idle duration to claim a message
// is determined by the MinDurationToClaim field of the ConsumerArgs struct.
// The Start field specifies the ID of the first message to claim, and the Count field
// determines the number of messages to claim in a batch. The Consumer field specifies
// the name of the consumer. If an error occurs during the claiming process, it is returned.
// If the error is not equal to the Valkey_NIL error, it is returned as is.
// Otherwise, the claimed messages are returned along with a nil error.
func (c *Consumer) AutoClaimMessages(ctx context.Context) ([]valkey.XRangeEntry, error) {
	cmd := c.Client.Instance.B().Xautoclaim().Key(c.StreamName).Group(c.GroupName).Consumer(c.ConsumerName).MinIdleTime(strconv.FormatInt(c.MinIdleAutoClaim, 10)).Start(c.nextIdAutoClaim).Count(*c.BatchSizeAutoClaim).Build()
	v, err := c.Client.Instance.Do(ctx, cmd).ToArray()
	if err != nil {
		return nil, err
	}

	nextMessage, err := v[0].ToString()
	if err != nil {
		return nil, err
	}

	c.nextIdAutoClaim = nextMessage

	e, err := v[1].AsXRange()
	if err != nil {
		return nil, err
	}

	return e, nil
}

// validateError checks if the given error contains a specific error message and performs an action accordingly.
// If the error message contains NOGROUP, it calls the createGroup method to create a group.
// Otherwise, it returns the original error.
func (c *Consumer) validateError(ctx context.Context, err error) error {
	if strings.Contains(err.Error(), consumer_NOGROUP) {
		return c.initGroup(ctx)
	}
	return err
}


// Consume consumes messages from Valkey.
// It first tries to fetch new messages, then pending messages, and finally claimed messages.
// If any messages are found, they are returned along with a nil error.
// If no messages are found, it returns nil and nil error.
func (c *Consumer) Consume(ctx context.Context) ([]valkey.XRangeEntry, error) {
	retry:
		var messages []valkey.XRangeEntry
		messages, err := c.NewMessages(ctx)
		if err != nil {
			err = c.validateError(ctx, err)
			if err == nil {
				goto retry
			}
			return nil, err
		}
		if len(messages) != 0 {
			return messages, nil
		}
	
		if c.BatchSizePending != nil {
			messages, err = c.PendingMessages(ctx)
			if err != nil {
				err = c.validateError(ctx, err)
				if err == nil {
					goto retry
				}
				return nil, err
			}
			if len(messages) != 0 {
				return messages, nil
			}
		}
	
		if c.BatchSizeAutoClaim != nil {
			messages, err = c.AutoClaimMessages(ctx)
			if err != nil {
				err = c.validateError(ctx, err)
				if err == nil {
					goto retry
				}
				return nil, err
			}
			if len(messages) != 0 {
				return messages, nil
			}
		}
	
		return nil, nil
	}