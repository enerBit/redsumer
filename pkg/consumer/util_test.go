package consumer

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestFilters(t *testing.T) {
	condition := GenerateTimeAndTriesCondition(minIdle)

	xPending := []redis.XPendingExt{
		{
			ID:         "1676389477-0",
			Consumer:   consumerName,
			Idle:       minIdle + 1*time.Second,
			RetryCount: 1,
		},
		{
			ID:         "1676389487-0",
			Consumer:   consumerName,
			Idle:       minIdle,
			RetryCount: 2,
		},
		{
			ID:         "1676389497-0",
			Consumer:   consumerName,
			Idle:       3*minIdle + 1*time.Second,
			RetryCount: 3,
		},
	}

	result := Filter(xPending, condition)

	t.Log(result)
	if len(result) != 2 {
		t.Error("Incomplete result set")
	}

	if result[0].ID != xPending[0].ID && result[1].ID != xPending[2].ID {
		t.Error("Incongruent response from filtering")
	}
}
