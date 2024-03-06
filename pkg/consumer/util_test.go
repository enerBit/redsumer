package consumer

import (
	"math"
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
			Idle:       minIdle + 1*time.Second,
			RetryCount: 3,
		},
		{
			ID:         "1676389499-0",
			Consumer:   consumerName,
			Idle:       minIdle + 1*time.Second + time.Duration(1000000*math.Exp(10)),
			RetryCount: 10,
		},
	}

	t.Log(xPending)
	result := Filter(xPending, condition)

	t.Log(result)
	if len(result) != 3 {
		t.Fatal("Incomplete result set")
	}

	if result[0].ID != xPending[0].ID && result[1].ID != xPending[2].ID {
		t.Error("Incongruent response from filtering")
	}
}

func TestFilterDead(t *testing.T) {
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
			Idle:       minIdle + 1*time.Second,
			RetryCount: 3,
		},
		{
			ID:         "1676389499-0",
			Consumer:   consumerName,
			Idle:       minIdle + 1*time.Second + time.Duration(1000000*math.Exp(10)),
			RetryCount: 10,
		},
	}

	t.Log(xPending)
	condition := ToleranceCondition(30 * time.Second)
	result := Filter(xPending, condition)

	t.Log(result)
	if len(result) != 1 {
		t.Fatal("Incomplete result set")
	}

	if result[0].ID != xPending[3].ID {
		t.Error("Incongruent response from filtering")
	}
}
