package consumer

import (
	"context"
	"strconv"
	"testing"

	"github.com/enerBit/redsumer/v3/pkg/client"
	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
)

const (
	streamName   string = "stream-test"
	groupName    string = "group-test"
	consumerName string = "consumer-test"
)

func TestCreateGroupSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("EXISTS", streamName)).Return(mock.Result(mock.ValkeyInt64(1)))
	db.EXPECT().Do(ctx, mock.Match("XGROUP", "CREATE", streamName, groupName, consumer_INITIAL_STREAM_ID)).Return(mock.ErrorResult(nil))

	clientArg := &client.ClientArgs{
		Instance: db,
	}
	c := &Consumer{
		Client:       clientArg,
		StreamName:   streamName,
		GroupName:    groupName,
		ConsumerName: consumerName,
		Tries:        []int{1},
	}

	err := c.initGroup(ctx)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestCreateGroupErrorBusyGroup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("EXISTS", streamName)).Return(mock.Result(mock.ValkeyInt64(1)))
	db.EXPECT().Do(ctx, mock.Match("XGROUP", "CREATE", streamName, groupName, consumer_INITIAL_STREAM_ID)).Return(mock.Result(mock.ValkeyError("BUSYGROUP Consumer Group name already exists")))

	clientArg := &client.ClientArgs{
		Instance: db,
	}
	c := &Consumer{
		Client:       clientArg,
		StreamName:   streamName,
		GroupName:    groupName,
		ConsumerName: consumerName,
		Tries:        []int{1},
	}

	err := c.initGroup(ctx)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestCreateGroupError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("EXISTS", streamName)).Return(mock.Result(mock.ValkeyInt64(1)))
	db.EXPECT().Do(ctx, mock.Match("XGROUP", "CREATE", streamName, groupName, consumer_INITIAL_STREAM_ID)).Return(mock.Result(mock.ValkeyError("error")))

	clientArg := &client.ClientArgs{
		Instance: db,
	}
	c := &Consumer{
		Client:       clientArg,
		StreamName:   streamName,
		GroupName:    groupName,
		ConsumerName: consumerName,
		Tries:        []int{1},
	}

	err := c.initGroup(ctx)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestWaitForStreamSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("EXISTS", streamName)).Return(mock.Result(mock.ValkeyInt64(0)))
	db.EXPECT().Do(ctx, mock.Match("EXISTS", streamName)).Return(mock.Result(mock.ValkeyInt64(0)))
	db.EXPECT().Do(ctx, mock.Match("EXISTS", streamName)).Return(mock.Result(mock.ValkeyInt64(0)))
	db.EXPECT().Do(ctx, mock.Match("EXISTS", streamName)).Return(mock.Result(mock.ValkeyInt64(1)))

	clientArg := &client.ClientArgs{
		Instance: db,
	}
	c := &Consumer{
		Client:       clientArg,
		StreamName:   streamName,
		GroupName:    groupName,
		ConsumerName: consumerName,
		Tries:        []int{1, 2, 3, 4},
	}

	err := c.waitForStream(ctx)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestWaitForStreamError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("EXISTS", streamName)).Return(mock.Result(mock.ValkeyInt64(0)))
	db.EXPECT().Do(ctx, mock.Match("EXISTS", streamName)).Return(mock.Result(mock.ValkeyInt64(0)))
	db.EXPECT().Do(ctx, mock.Match("EXISTS", streamName)).Return(mock.Result(mock.ValkeyInt64(0)))
	db.EXPECT().Do(ctx, mock.Match("EXISTS", streamName)).Return(mock.Result(mock.ValkeyInt64(0)))

	clientArg := &client.ClientArgs{
		Instance: db,
	}
	c := &Consumer{
		Client:       clientArg,
		StreamName:   streamName,
		GroupName:    groupName,
		ConsumerName: consumerName,
		Tries:        []int{1, 2, 3, 4},
	}

	err := c.waitForStream(ctx)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestStillMineSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	db := mock.NewClient(ctrl)

	messageId := "1676389477-0"
	var idleStillMine int64 = 1

	db.EXPECT().Do(ctx, mock.Match("XPENDING", streamName, groupName, "IDLE", strconv.FormatInt(idleStillMine, 10), messageId, messageId, "1", consumerName)).Return(mock.Result(mock.ValkeyArray(valkey.ValkeyMessage{})))
	clientArg := &client.ClientArgs{
		Instance: db,
	}
	c := &Consumer{
		Client:        clientArg,
		StreamName:    streamName,
		GroupName:     groupName,
		ConsumerName:  consumerName,
		IdleStillMine: idleStillMine,
	}

	ok, err := c.StillMine(ctx, messageId)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if !ok {
		t.Fatalf("expected true, got false")
	}
}

func TestStillMineError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	db := mock.NewClient(ctrl)

	messageId := "1676389477-0"
	var idleStillMine int64 = 1

	db.EXPECT().Do(ctx, mock.Match("XPENDING", streamName, groupName, "IDLE", strconv.FormatInt(idleStillMine, 10), messageId, messageId, "1", consumerName)).Return(mock.Result(mock.ValkeyError("error")))
	clientArg := &client.ClientArgs{
		Instance: db,
	}
	c := &Consumer{
		Client:        clientArg,
		StreamName:    streamName,
		GroupName:     groupName,
		ConsumerName:  consumerName,
		IdleStillMine: idleStillMine,
	}

	_, err := c.StillMine(ctx, messageId)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestStillMineFalse(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	db := mock.NewClient(ctrl)

	messageId := "1676389477-0"
	var idleStillMine int64 = 1

	db.EXPECT().Do(ctx, mock.Match("XPENDING", streamName, groupName, "IDLE", strconv.FormatInt(idleStillMine, 10), messageId, messageId, "1", consumerName)).Return(mock.Result(mock.ValkeyArray()))
	clientArg := &client.ClientArgs{
		Instance: db,
	}
	c := &Consumer{
		Client:        clientArg,
		StreamName:    streamName,
		GroupName:     groupName,
		ConsumerName:  consumerName,
		IdleStillMine: idleStillMine,
	}

	ok, err := c.StillMine(ctx, messageId)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if ok {
		t.Fatalf("expected false, got true")
	}
}

func TestAckSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	db := mock.NewClient(ctrl)

	messageId := "1676389477-0"

	db.EXPECT().Do(ctx, mock.Match("XACK", streamName, groupName, messageId)).Return(mock.Result(mock.ValkeyInt64(1)))
	clientArg := &client.ClientArgs{
		Instance: db,
	}
	c := &Consumer{
		Client:       clientArg,
		StreamName:   streamName,
		GroupName:    groupName,
		ConsumerName: consumerName,
	}

	err := c.AcknowledgeMessage(ctx, messageId)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestAckError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	db := mock.NewClient(ctrl)

	messageId := "1676389477-0"

	db.EXPECT().Do(ctx, mock.Match("XACK", streamName, groupName, messageId)).Return(mock.Result(mock.ValkeyError("error")))
	clientArg := &client.ClientArgs{
		Instance: db,
	}
	c := &Consumer{
		Client:       clientArg,
		StreamName:   streamName,
		GroupName:    groupName,
		ConsumerName: consumerName,
	}

	err := c.AcknowledgeMessage(ctx, messageId)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestNewMessagesSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("XREADGROUP", "GROUP", groupName, consumerName, "COUNT", "1", "STREAMS", streamName, consumer_NEVER_DELIVERED_TO_OTHER_CONSUMERS_SO_FAR)).Return(mock.Result(mock.ValkeyArray(mock.ValkeyArray(mock.ValkeyNil(), mock.ValkeyNil()))))
	clientArg := &client.ClientArgs{
		Instance: db,
	}
	c := &Consumer{
		Client:              clientArg,
		StreamName:          streamName,
		GroupName:           groupName,
		ConsumerName:        consumerName,
		BatchSizeNewMessage: 1,
	}

	_, err := c.NewMessages(ctx)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestNewMessagesError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("XREADGROUP", "GROUP", groupName, consumerName, "COUNT", "1", "STREAMS", streamName, consumer_NEVER_DELIVERED_TO_OTHER_CONSUMERS_SO_FAR)).Return(mock.Result(mock.ValkeyError("error")))
	clientArg := &client.ClientArgs{
		Instance: db,
	}
	c := &Consumer{
		Client:              clientArg,
		StreamName:          streamName,
		GroupName:           groupName,
		ConsumerName:        consumerName,
		BatchSizeNewMessage: 1,
	}

	_, err := c.NewMessages(ctx)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestPendingMessagesSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("XREADGROUP", "GROUP", groupName, consumerName, "COUNT", "1", "STREAMS", streamName, consumer_INITIAL_STREAM_ID)).Return(mock.Result(mock.ValkeyArray(mock.ValkeyArray(mock.ValkeyNil(), mock.ValkeyNil()))))
	clientArg := &client.ClientArgs{
		Instance: db,
	}

	var S int64 = 1
	c := &Consumer{
		Client:                 clientArg,
		StreamName:             streamName,
		GroupName:              groupName,
		ConsumerName:           consumerName,
		BatchSizePending:       &S,
		latestPendingMessageId: consumer_INITIAL_STREAM_ID,
	}

	_, err := c.PendingMessages(ctx)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestPendingMessagesError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("XREADGROUP", "GROUP", groupName, consumerName, "COUNT", "1", "STREAMS", streamName, consumer_INITIAL_STREAM_ID)).Return(mock.Result(mock.ValkeyError("error")))
	clientArg := &client.ClientArgs{
		Instance: db,
	}

	var S int64 = 1
	c := &Consumer{
		Client:                 clientArg,
		StreamName:             streamName,
		GroupName:              groupName,
		ConsumerName:           consumerName,
		BatchSizePending:       &S,
		latestPendingMessageId: consumer_INITIAL_STREAM_ID,
	}

	_, err := c.PendingMessages(ctx)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestAutoClaimedSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	db := mock.NewClient(ctrl)

	messageId := "1676389477-0"
	db.EXPECT().Do(ctx, mock.Match("XAUTOCLAIM", streamName, groupName, consumerName, "100", consumer_INITIAL_STREAM_ID, "COUNT", "1")).Return(mock.Result(mock.ValkeyArray(mock.ValkeyString(messageId), mock.ValkeyArray(mock.ValkeyArray(mock.ValkeyString(messageId), mock.ValkeyMap(make(map[string]valkey.ValkeyMessage)))))))
	clientArg := &client.ClientArgs{
		Instance: db,
	}
	var S int64 = 1
	c := &Consumer{
		Client:             clientArg,
		StreamName:         streamName,
		GroupName:          groupName,
		ConsumerName:       consumerName,
		MinIdleAutoClaim:   100,
		nextIdAutoClaim:    consumer_INITIAL_STREAM_ID,
		BatchSizeAutoClaim: &S,
	}

	_, err := c.AutoClaimMessages(ctx)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestAutoClaimedError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("XAUTOCLAIM", streamName, groupName, consumerName, "100", consumer_INITIAL_STREAM_ID, "COUNT", "1")).Return(mock.Result(mock.ValkeyError("error")))
	clientArg := &client.ClientArgs{
		Instance: db,
	}
	var S int64 = 1
	c := &Consumer{
		Client:             clientArg,
		StreamName:         streamName,
		GroupName:          groupName,
		ConsumerName:       consumerName,
		MinIdleAutoClaim:   100,
		nextIdAutoClaim:    consumer_INITIAL_STREAM_ID,
		BatchSizeAutoClaim: &S,
	}

	_, err := c.AutoClaimMessages(ctx)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}
