package consumer

import (
	"time"

	"github.com/redis/go-redis/v9"
)

func GenerateTimeAndTriesCondition(threshold time.Duration) func(redis.XPendingExt) bool {
	return func(msg redis.XPendingExt) bool {
		return (msg.RetryCount * threshold.Milliseconds()) < msg.Idle.Milliseconds()
	}
}

func Filter(items []redis.XPendingExt, conndition func(item redis.XPendingExt) bool) []redis.XPendingExt {
	filteredItems := make([]redis.XPendingExt, 0, len(items))

	for _, val := range items {
		if conndition(val) {
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
