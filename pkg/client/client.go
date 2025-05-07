package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/valkey-io/valkey-go"
)

type ClientArgs struct {
	Host     string
	Port     string
	Instance valkey.Client
}

var clientCache = make(map[string]valkey.Client)
var cacheMutex = sync.RWMutex{}

// InitClient creates a new Valkey client or reuses an existing one from the cache.
// It takes a context.Context as input and uses the ClientArgs receiver to access the Host and Port fields.
// The Valkey client is created with the specified Valkey address if not already in the cache.
// It then sends a PING command to the Valkey server to check the connection.
// The function returns any error encountered during client creation or the PING command.
func (r *ClientArgs) InitClient(ctx context.Context) error {
	redisAddress := fmt.Sprintf("%s:%s", r.Host, r.Port)

	// Check if we already have a client for this address
	cacheMutex.RLock()
	cachedClient, exists := clientCache[redisAddress]
	cacheMutex.RUnlock()

	if exists {
		r.Instance = cachedClient
		return nil
	}

	// Create a new client if none exists
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{redisAddress}})
	if err != nil {
		return err
	}

	// Test the connection
	err = client.Do(ctx, client.B().Ping().Build()).Error()
	if err != nil {
		return err
	}

	// Store the client in the cache
	cacheMutex.Lock()
	clientCache[redisAddress] = client
	cacheMutex.Unlock()

	r.Instance = client
	return nil
}
