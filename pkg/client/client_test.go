package client

import (
	"context"
	"testing"
)

func TestNewRedisClient(t *testing.T) {
	ctx := context.Background()

	c := ClientArgs{
		Host: "localhost",
		Port: "6378",
	}

	err := c.InitClient(ctx)
	if err != nil {
		t.Errorf("Error: %v", err)
	}
}
