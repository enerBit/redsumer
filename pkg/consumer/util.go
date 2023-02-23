package consumer

import (
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

func GenerateTimeAndTriesCondition(threshold time.Duration) func(redis.XPendingExt) bool {
	return func(msg redis.XPendingExt) bool {
		v := float64(threshold.Milliseconds()) + math.Exp(float64(msg.RetryCount))
		return v < float64(msg.Idle.Milliseconds())
	}
}

func Filter(items []redis.XPendingExt, condition func(item redis.XPendingExt) bool) []redis.XPendingExt {
	filteredItems := make([]redis.XPendingExt, 0, len(items))

	for _, val := range items {
		if condition(val) {
			filteredItems = append(filteredItems, val)
		}
	}

	return filteredItems
}

func getIds(msgs []redis.XPendingExt) []string {

	ids := make([]string, len(msgs))

	for i, val := range msgs {
		ids[i] = val.ID
	}

	return ids
}
