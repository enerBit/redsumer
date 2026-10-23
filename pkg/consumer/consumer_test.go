package consumer

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/enerBit/redsumer/v4/pkg/client"
	errors_custom "github.com/enerBit/redsumer/v4/pkg/errors"
	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
)

const (
	streamName   string = "stream-test"
	groupName    string = "group-test"
	consumerName string = "consumer-test"
)

// baseConsumer returns a Consumer with minimal valid configuration and initialized
// internal state for tests (mirrors what InitConsumer sets up).
func baseConsumer(db valkey.Client) *Consumer {
	return &Consumer{
		Client:         &client.ClientArgs{Instance: db},
		StreamName:     streamName,
		GroupName:      groupName,
		ConsumerName:   consumerName,
		Tries:          []int{0},
		BatchSize:      1,
		ClaimBatch:     1,
		BlockMs:        0,
		ClaimMinIdleMs: 100,
		RatioSlice:     []int{5},
		PelWaitSlice:   []int{0},
		BackoffSlice:   []int{0},
		// internal state (mirrors InitConsumer initialization)
		nextIdAutoClaim: consumer_INITIAL_STREAM_ID,
		prevPelSize:     -1,
	}
}

// ── Group / stream init ───────────────────────────────────────────────────────

func TestCreateGroupSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("EXISTS", streamName)).Return(mock.Result(mock.ValkeyInt64(1)))
	db.EXPECT().Do(ctx, mock.Match("XGROUP", "CREATE", streamName, groupName, consumer_INITIAL_STREAM_ID)).Return(mock.ErrorResult(nil))

	if err := baseConsumer(db).initGroup(ctx); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestCreateGroupErrorBusyGroup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("EXISTS", streamName)).Return(mock.Result(mock.ValkeyInt64(1)))
	db.EXPECT().Do(ctx, mock.Match("XGROUP", "CREATE", streamName, groupName, consumer_INITIAL_STREAM_ID)).Return(mock.Result(mock.ValkeyError("BUSYGROUP Consumer Group name already exists")))

	if err := baseConsumer(db).initGroup(ctx); err != nil {
		t.Fatalf("expected nil for BUSYGROUP, got %v", err)
	}
}

func TestCreateGroupError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("EXISTS", streamName)).Return(mock.Result(mock.ValkeyInt64(1)))
	db.EXPECT().Do(ctx, mock.Match("XGROUP", "CREATE", streamName, groupName, consumer_INITIAL_STREAM_ID)).Return(mock.Result(mock.ValkeyError("error")))

	if err := baseConsumer(db).initGroup(ctx); err == nil {
		t.Fatal("expected error, got nil")
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

	c := baseConsumer(db)
	c.Tries = []int{0, 0, 0, 0}
	if err := c.waitForStream(ctx); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestWaitForStreamError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("EXISTS", streamName)).Return(mock.Result(mock.ValkeyInt64(0))).Times(4)

	c := baseConsumer(db)
	c.Tries = []int{0, 0, 0, 0}
	if err := c.waitForStream(ctx); err == nil {
		t.Fatal("expected error, got nil")
	}
}

// ── validateConfig ────────────────────────────────────────────────────────────

// validConfig returns a Consumer that passes validateConfig. Each test breaks a
// single field so it fails for the reason the test names.
func validConfig() *Consumer {
	return &Consumer{
		Client:       &client.ClientArgs{},
		StreamName:   streamName,
		GroupName:    groupName,
		ConsumerName: consumerName,
		BatchSize:    1,
		ClaimBatch:   1,
		RatioSlice:   []int{5},
		PelWaitSlice: []int{0},
		BackoffSlice: []int{0},
	}
}

func TestInitConsumer_NilClient(t *testing.T) {
	c := validConfig()
	c.Client = nil
	if !errors.Is(validateConfig(c), errors_custom.ErrInvalidConfig) {
		t.Fatal("expected ErrInvalidConfig for nil Client")
	}
}

func TestInitConsumer_EmptyStreamName(t *testing.T) {
	c := validConfig()
	c.StreamName = " "
	if !errors.Is(validateConfig(c), errors_custom.ErrInvalidConfig) {
		t.Fatal("expected ErrInvalidConfig for empty StreamName")
	}
}

func TestInitConsumer_EmptyGroupName(t *testing.T) {
	c := validConfig()
	c.GroupName = ""
	if !errors.Is(validateConfig(c), errors_custom.ErrInvalidConfig) {
		t.Fatal("expected ErrInvalidConfig for empty GroupName")
	}
}

func TestInitConsumer_EmptyConsumerName(t *testing.T) {
	c := validConfig()
	c.ConsumerName = ""
	if !errors.Is(validateConfig(c), errors_custom.ErrInvalidConfig) {
		t.Fatal("expected ErrInvalidConfig for empty ConsumerName")
	}
}

func TestInitConsumer_EmptyRatioSlice(t *testing.T) {
	c := validConfig()
	c.RatioSlice = []int{}
	if !errors.Is(validateConfig(c), errors_custom.ErrInvalidConfig) {
		t.Fatal("expected ErrInvalidConfig for empty RatioSlice")
	}
}

func TestInitConsumer_EmptyPelWaitSlice(t *testing.T) {
	c := validConfig()
	c.PelWaitSlice = []int{}
	if !errors.Is(validateConfig(c), errors_custom.ErrInvalidConfig) {
		t.Fatal("expected ErrInvalidConfig for empty PelWaitSlice")
	}
}

func TestInitConsumer_EmptyBackoffSlice(t *testing.T) {
	c := validConfig()
	c.BackoffSlice = []int{}
	if !errors.Is(validateConfig(c), errors_custom.ErrInvalidConfig) {
		t.Fatal("expected ErrInvalidConfig for empty BackoffSlice")
	}
}

func TestInitConsumer_ZeroBatchSize(t *testing.T) {
	c := validConfig()
	c.BatchSize = 0
	if !errors.Is(validateConfig(c), errors_custom.ErrInvalidConfig) {
		t.Fatal("expected ErrInvalidConfig for BatchSize=0")
	}
}

func TestInitConsumer_ZeroClaimBatch(t *testing.T) {
	c := validConfig()
	c.ClaimBatch = 0
	if !errors.Is(validateConfig(c), errors_custom.ErrInvalidConfig) {
		t.Fatal("expected ErrInvalidConfig for ClaimBatch=0")
	}
}

func TestInitConsumer_ValidConfig(t *testing.T) {
	if err := validateConfig(validConfig()); err != nil {
		t.Fatalf("expected nil for valid config, got %v", err)
	}
}

// ── StillMine ─────────────────────────────────────────────────────────────────

func TestStillMineSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	messageId := "1676389477-0"
	var idle int64 = 1

	db.EXPECT().Do(ctx, mock.Match("XPENDING", streamName, groupName, "IDLE", strconv.FormatInt(idle, 10), messageId, messageId, "1", consumerName)).
		Return(mock.Result(mock.ValkeyArray(valkey.ValkeyMessage{})))

	c := baseConsumer(db)
	c.IdleStillMine = idle

	ok, err := c.StillMine(ctx, messageId)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if !ok {
		t.Fatal("expected true, got false")
	}
}

func TestStillMineError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	messageId := "1676389477-0"
	var idle int64 = 1

	db.EXPECT().Do(ctx, mock.Match("XPENDING", streamName, groupName, "IDLE", strconv.FormatInt(idle, 10), messageId, messageId, "1", consumerName)).
		Return(mock.Result(mock.ValkeyError("error")))

	c := baseConsumer(db)
	c.IdleStillMine = idle

	_, err := c.StillMine(ctx, messageId)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestStillMineFalse(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	messageId := "1676389477-0"
	var idle int64 = 1

	db.EXPECT().Do(ctx, mock.Match("XPENDING", streamName, groupName, "IDLE", strconv.FormatInt(idle, 10), messageId, messageId, "1", consumerName)).
		Return(mock.Result(mock.ValkeyArray()))

	c := baseConsumer(db)
	c.IdleStillMine = idle

	ok, err := c.StillMine(ctx, messageId)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if ok {
		t.Fatal("expected false, got true")
	}
}

// ── AcknowledgeMessage ────────────────────────────────────────────────────────

func TestAckSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	messageId := "1676389477-0"
	db.EXPECT().Do(ctx, mock.Match("XACK", streamName, groupName, messageId)).Return(mock.Result(mock.ValkeyInt64(1)))

	if err := baseConsumer(db).AcknowledgeMessage(ctx, messageId); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestAckError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	messageId := "1676389477-0"
	db.EXPECT().Do(ctx, mock.Match("XACK", streamName, groupName, messageId)).Return(mock.Result(mock.ValkeyError("error")))

	if err := baseConsumer(db).AcknowledgeMessage(ctx, messageId); err == nil {
		t.Fatal("expected error, got nil")
	}
}

// ── newMessages ───────────────────────────────────────────────────────────────

func TestNewMessagesSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("XREADGROUP", "GROUP", groupName, consumerName, "COUNT", "1", "BLOCK", "0", "STREAMS", streamName, consumer_NEVER_DELIVERED_TO_OTHER_CONSUMERS_SO_FAR)).
		Return(mock.Result(mock.ValkeyArray(mock.ValkeyArray(mock.ValkeyNil(), mock.ValkeyNil()))))

	_, err := baseConsumer(db).newMessages(ctx)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestNewMessagesTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("XREADGROUP", "GROUP", groupName, consumerName, "COUNT", "1", "BLOCK", "0", "STREAMS", streamName, consumer_NEVER_DELIVERED_TO_OTHER_CONSUMERS_SO_FAR)).
		Return(mock.Result(mock.ValkeyNil()))

	msgs, err := baseConsumer(db).newMessages(ctx)
	if err != nil {
		t.Fatalf("block timeout must not be an error, got %v", err)
	}
	if msgs != nil {
		t.Fatalf("expected nil msgs on timeout, got %v", msgs)
	}
}

func TestNewMessagesError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("XREADGROUP", "GROUP", groupName, consumerName, "COUNT", "1", "BLOCK", "0", "STREAMS", streamName, consumer_NEVER_DELIVERED_TO_OTHER_CONSUMERS_SO_FAR)).
		Return(mock.Result(mock.ValkeyError("error")))

	_, err := baseConsumer(db).newMessages(ctx)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// ── autoClaimMessages ─────────────────────────────────────────────────────────

func TestAutoClaimMessagesSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	messageId := "1676389477-0"
	nextCursor := "9999-0"
	db.EXPECT().Do(ctx, mock.Match("XAUTOCLAIM", streamName, groupName, consumerName, "100", consumer_INITIAL_STREAM_ID, "COUNT", "1")).
		Return(mock.Result(mock.ValkeyArray(
			mock.ValkeyString(nextCursor),
			mock.ValkeyArray(mock.ValkeyArray(mock.ValkeyString(messageId), mock.ValkeyMap(make(map[string]valkey.ValkeyMessage)))),
		)))

	c := baseConsumer(db)
	c.nextIdAutoClaim = consumer_INITIAL_STREAM_ID

	_, err := c.autoClaimMessages(ctx)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if c.pelTraversalComplete {
		t.Fatal("expected pelTraversalComplete=false when cursor != 0-0")
	}
	if c.nextIdAutoClaim != nextCursor {
		t.Fatalf("expected cursor %q, got %q", nextCursor, c.nextIdAutoClaim)
	}
}

func TestAutoClaimMessagesCursorReset(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	messageId := "1676389477-0"
	db.EXPECT().Do(ctx, mock.Match("XAUTOCLAIM", streamName, groupName, consumerName, "100", consumer_INITIAL_STREAM_ID, "COUNT", "1")).
		Return(mock.Result(mock.ValkeyArray(
			mock.ValkeyString(consumer_INITIAL_STREAM_ID),
			mock.ValkeyArray(mock.ValkeyArray(mock.ValkeyString(messageId), mock.ValkeyMap(make(map[string]valkey.ValkeyMessage)))),
		)))

	c := baseConsumer(db)
	c.nextIdAutoClaim = consumer_INITIAL_STREAM_ID

	_, err := c.autoClaimMessages(ctx)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if !c.pelTraversalComplete {
		t.Fatal("expected pelTraversalComplete=true when cursor resets to 0-0")
	}
}

func TestAutoClaimMessagesError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("XAUTOCLAIM", streamName, groupName, consumerName, "100", consumer_INITIAL_STREAM_ID, "COUNT", "1")).
		Return(mock.Result(mock.ValkeyError("error")))

	c := baseConsumer(db)
	c.nextIdAutoClaim = consumer_INITIAL_STREAM_ID

	_, err := c.autoClaimMessages(ctx)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// ── pelSize ───────────────────────────────────────────────────────────────────

func TestPelSizeSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("XPENDING", streamName, groupName)).
		Return(mock.Result(mock.ValkeyArray(mock.ValkeyInt64(5), mock.ValkeyNil(), mock.ValkeyNil(), mock.ValkeyNil())))

	size, err := baseConsumer(db).pelSize(ctx)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if size != 5 {
		t.Fatalf("expected 5, got %d", size)
	}
}

func TestPelSizeZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("XPENDING", streamName, groupName)).
		Return(mock.Result(mock.ValkeyArray(mock.ValkeyInt64(0), mock.ValkeyNil(), mock.ValkeyNil(), mock.ValkeyNil())))

	size, err := baseConsumer(db).pelSize(ctx)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if size != 0 {
		t.Fatalf("expected 0, got %d", size)
	}
}

func TestPelSizeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, mock.Match("XPENDING", streamName, groupName)).
		Return(mock.Result(mock.ValkeyError("error")))

	_, err := baseConsumer(db).pelSize(ctx)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// ── contextSleep ──────────────────────────────────────────────────────────────

func TestContextSleep_Expiry(t *testing.T) {
	if err := contextSleep(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestContextSleep_Cancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := contextSleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestContextSleep_ZeroDuration(t *testing.T) {
	if err := contextSleep(context.Background(), 0); err != nil {
		t.Fatalf("expected nil for zero duration, got %v", err)
	}
}

// ── advanceIdx ────────────────────────────────────────────────────────────────

func TestAdvanceIdx_BelowMax(t *testing.T) {
	if advanceIdx(2, 4) != 3 {
		t.Fatal("expected 3")
	}
}

func TestAdvanceIdx_AtMax(t *testing.T) {
	if advanceIdx(4, 4) != 4 {
		t.Fatal("expected 4 (stay at max)")
	}
}

func TestAdvanceIdx_ZeroMax(t *testing.T) {
	if advanceIdx(0, 0) != 0 {
		t.Fatal("expected 0")
	}
}

// ── Stats ─────────────────────────────────────────────────────────────────────

func TestStats(t *testing.T) {
	c := &Consumer{ratioIdx: 1, pelWaitIdx: 2, backoffIdx: 3, prevPelSize: 42}
	s := c.Stats()
	if s.RatioIdx != 1 || s.PelWaitIdx != 2 || s.BackoffIdx != 3 || s.PrevPelSize != 42 {
		t.Fatalf("unexpected stats: %+v", s)
	}
}

// ── DefaultConsumerName ───────────────────────────────────────────────────────

func TestDefaultConsumerName(t *testing.T) {
	name := DefaultConsumerName()
	if name == "" {
		t.Fatal("expected non-empty consumer name")
	}
	for i, ch := range name {
		if ch == '-' && i > 0 {
			return
		}
	}
	t.Fatalf("expected hostname-pid format, got %q", name)
}

// ── Consume helpers ───────────────────────────────────────────────────────────

// emptyXRead simulates a XREADGROUP block timeout (nil reply = no messages).
func emptyXRead() valkey.ValkeyResult {
	return mock.Result(mock.ValkeyNil())
}

// oneMessageXRead simulates XREADGROUP returning one message for the stream.
func oneMessageXRead(msgID string) valkey.ValkeyResult {
	return mock.Result(mock.ValkeyArray(
		mock.ValkeyArray(
			mock.ValkeyString(streamName),
			mock.ValkeyArray(
				mock.ValkeyArray(mock.ValkeyString(msgID), mock.ValkeyMap(map[string]valkey.ValkeyMessage{})),
			),
		),
	))
}

// oneMessageXAutoclaim simulates XAUTOCLAIM returning one message and a next cursor.
func oneMessageXAutoclaim(msgID, nextCursor string) valkey.ValkeyResult {
	return mock.Result(mock.ValkeyArray(
		mock.ValkeyString(nextCursor),
		mock.ValkeyArray(mock.ValkeyArray(mock.ValkeyString(msgID), mock.ValkeyMap(make(map[string]valkey.ValkeyMessage)))),
	))
}

// emptyXAutoclaim simulates XAUTOCLAIM returning no messages with a given next cursor.
func emptyXAutoclaim(nextCursor string) valkey.ValkeyResult {
	return mock.Result(mock.ValkeyArray(
		mock.ValkeyString(nextCursor),
		mock.ValkeyArray(),
	))
}

// pelSummaryResult simulates XPENDING summary with a given pending count.
func pelSummaryResult(count int64) valkey.ValkeyResult {
	return mock.Result(mock.ValkeyArray(mock.ValkeyInt64(count), mock.ValkeyNil(), mock.ValkeyNil(), mock.ValkeyNil()))
}

func xreadgroupMatcher() gomock.Matcher {
	return mock.Match("XREADGROUP", "GROUP", groupName, consumerName, "COUNT", "1", "BLOCK", strconv.FormatInt(0, 10), "STREAMS", streamName, consumer_NEVER_DELIVERED_TO_OTHER_CONSUMERS_SO_FAR)
}

func xautoclaimMatcher(startID string) gomock.Matcher {
	return mock.Match("XAUTOCLAIM", streamName, groupName, consumerName, "100", startID, "COUNT", "1")
}

func xpendingSummaryMatcher() gomock.Matcher {
	return mock.Match("XPENDING", streamName, groupName)
}

// ── Consume – adaptive loop ───────────────────────────────────────────────────

// TestConsume_NewMessages_BelowRatio: new messages below ratio → PEL not triggered.
func TestConsume_NewMessages_BelowRatio(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, xreadgroupMatcher()).Return(oneMessageXRead("1-0"))

	c := baseConsumer(db)
	c.RatioSlice = []int{5}

	msgs, err := c.Consume(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if c.newMsgCounter != 1 {
		t.Fatalf("expected newMsgCounter=1, got %d", c.newMsgCounter)
	}
}

// TestConsume_RatioMet_TriggersPEL: reaching the ratio threshold triggers XAUTOCLAIM.
func TestConsume_RatioMet_TriggersPEL(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, xreadgroupMatcher()).Return(oneMessageXRead("1-0"))
	db.EXPECT().Do(ctx, xautoclaimMatcher(consumer_INITIAL_STREAM_ID)).Return(emptyXAutoclaim(consumer_INITIAL_STREAM_ID))
	db.EXPECT().Do(ctx, xpendingSummaryMatcher()).Return(pelSummaryResult(0))

	c := baseConsumer(db)
	c.RatioSlice = []int{1} // every single new message triggers PEL

	msgs, err := c.Consume(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if c.newMsgCounter != 0 {
		t.Fatalf("expected newMsgCounter reset to 0, got %d", c.newMsgCounter)
	}
}

// TestConsume_NoNewMessages_TriggersPEL: empty XREADGROUP immediately triggers PEL.
func TestConsume_NoNewMessages_TriggersPEL(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, xreadgroupMatcher()).Return(emptyXRead())
	db.EXPECT().Do(ctx, xautoclaimMatcher(consumer_INITIAL_STREAM_ID)).Return(oneMessageXAutoclaim("2-0", "9-0"))

	c := baseConsumer(db)

	msgs, err := c.Consume(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 PEL message, got %d", len(msgs))
	}
}

// TestConsume_BothEmpty_Backoff: both phases empty → backoff index advances.
func TestConsume_BothEmpty_Backoff(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, xreadgroupMatcher()).Return(emptyXRead())
	db.EXPECT().Do(ctx, xautoclaimMatcher(consumer_INITIAL_STREAM_ID)).Return(emptyXAutoclaim(consumer_INITIAL_STREAM_ID))
	db.EXPECT().Do(ctx, xpendingSummaryMatcher()).Return(pelSummaryResult(0))

	c := baseConsumer(db)
	c.BackoffSlice = []int{0, 0, 0}

	msgs, err := c.Consume(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msgs != nil {
		t.Fatalf("expected nil msgs, got %v", msgs)
	}
	if c.backoffIdx != 1 {
		t.Fatalf("expected backoffIdx=1, got %d", c.backoffIdx)
	}
}

// TestConsume_BackoffAtMax_StaysAtMax: backoffIdx does not exceed slice bounds.
func TestConsume_BackoffAtMax_StaysAtMax(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, xreadgroupMatcher()).Return(emptyXRead())
	db.EXPECT().Do(ctx, xautoclaimMatcher(consumer_INITIAL_STREAM_ID)).Return(emptyXAutoclaim(consumer_INITIAL_STREAM_ID))
	db.EXPECT().Do(ctx, xpendingSummaryMatcher()).Return(pelSummaryResult(0))

	c := baseConsumer(db)
	c.BackoffSlice = []int{0} // single value; must stay at index 0

	c.Consume(ctx) //nolint:errcheck
	if c.backoffIdx != 0 {
		t.Fatalf("expected backoffIdx=0 (at max), got %d", c.backoffIdx)
	}
}

// TestConsume_BackoffResets: backoffIdx resets when new messages arrive.
func TestConsume_BackoffResets(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, xreadgroupMatcher()).Return(oneMessageXRead("1-0"))

	c := baseConsumer(db)
	c.backoffIdx = 3
	c.RatioSlice = []int{5} // ratio not met; no PEL call expected

	c.Consume(ctx) //nolint:errcheck
	if c.backoffIdx != 0 {
		t.Fatalf("expected backoffIdx reset to 0, got %d", c.backoffIdx)
	}
}

// TestConsume_PELTraversal_SizeChanged_ResetsIndices: PEL size change resets protection indices.
func TestConsume_PELTraversal_SizeChanged_ResetsIndices(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, xreadgroupMatcher()).Return(emptyXRead())
	db.EXPECT().Do(ctx, xautoclaimMatcher(consumer_INITIAL_STREAM_ID)).Return(emptyXAutoclaim(consumer_INITIAL_STREAM_ID))
	db.EXPECT().Do(ctx, xpendingSummaryMatcher()).Return(pelSummaryResult(8)) // changed from 10

	c := baseConsumer(db)
	c.RatioSlice = []int{5, 10, 20, 50}
	c.PelWaitSlice = []int{0, 0, 0, 0} // zero to keep test fast; index behavior is what we're testing
	c.ratioIdx = 2
	c.pelWaitIdx = 2
	c.prevPelSize = 10

	c.Consume(ctx) //nolint:errcheck

	if c.ratioIdx != 0 || c.pelWaitIdx != 0 {
		t.Fatalf("expected indices reset to 0, got ratioIdx=%d pelWaitIdx=%d", c.ratioIdx, c.pelWaitIdx)
	}
	if c.prevPelSize != 8 {
		t.Fatalf("expected prevPelSize=8, got %d", c.prevPelSize)
	}
}

// TestConsume_PELTraversal_SizeUnchanged_AdvancesIndices: unchanged PEL size advances indices.
func TestConsume_PELTraversal_SizeUnchanged_AdvancesIndices(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, xreadgroupMatcher()).Return(emptyXRead())
	db.EXPECT().Do(ctx, xautoclaimMatcher(consumer_INITIAL_STREAM_ID)).Return(emptyXAutoclaim(consumer_INITIAL_STREAM_ID))
	db.EXPECT().Do(ctx, xpendingSummaryMatcher()).Return(pelSummaryResult(10)) // same as prevPelSize

	c := baseConsumer(db)
	c.RatioSlice = []int{5, 10, 20, 50}
	c.PelWaitSlice = []int{0, 0, 0, 0} // zero to keep test fast; index behavior is what we're testing
	c.ratioIdx = 0
	c.pelWaitIdx = 0
	c.prevPelSize = 10

	c.Consume(ctx) //nolint:errcheck

	if c.ratioIdx != 1 || c.pelWaitIdx != 1 {
		t.Fatalf("expected indices advanced to 1, got ratioIdx=%d pelWaitIdx=%d", c.ratioIdx, c.pelWaitIdx)
	}
}

// TestConsume_PELTraversal_AtMax_StaysAtMax: protection indices do not exceed slice bounds.
func TestConsume_PELTraversal_AtMax_StaysAtMax(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, xreadgroupMatcher()).Return(emptyXRead())
	db.EXPECT().Do(ctx, xautoclaimMatcher(consumer_INITIAL_STREAM_ID)).Return(emptyXAutoclaim(consumer_INITIAL_STREAM_ID))
	db.EXPECT().Do(ctx, xpendingSummaryMatcher()).Return(pelSummaryResult(10))

	c := baseConsumer(db)
	c.RatioSlice = []int{5, 50}
	c.PelWaitSlice = []int{0, 0} // zero to keep test fast; index behavior is what we're testing
	c.ratioIdx = 1               // already at max
	c.pelWaitIdx = 1             // already at max
	c.prevPelSize = 10

	c.Consume(ctx) //nolint:errcheck

	if c.ratioIdx != 1 || c.pelWaitIdx != 1 {
		t.Fatalf("expected indices to stay at max=1, got ratioIdx=%d pelWaitIdx=%d", c.ratioIdx, c.pelWaitIdx)
	}
}

// TestConsume_ContextCancelledDuringBackoff: context cancellation during backoff returns error.
func TestConsume_ContextCancelledDuringBackoff(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx, cancel := context.WithCancel(context.Background())
	db := mock.NewClient(ctrl)

	db.EXPECT().Do(ctx, xreadgroupMatcher()).Return(emptyXRead())
	db.EXPECT().Do(ctx, xautoclaimMatcher(consumer_INITIAL_STREAM_ID)).Return(emptyXAutoclaim(consumer_INITIAL_STREAM_ID))
	db.EXPECT().Do(ctx, xpendingSummaryMatcher()).Return(pelSummaryResult(0))

	cancel() // cancel before Consume runs so the backoff sleep exits immediately

	c := baseConsumer(db)
	c.BackoffSlice = []int{60} // long sleep; would block without cancellation

	_, err := c.Consume(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
