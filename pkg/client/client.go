package client

import (
	"context"
	"fmt"

	"github.com/valkey-io/valkey-go"
)

type ClientArgs struct {
	Host     string
	Port     string
	Instance valkey.Client
}

// var once sync.Once

// newValkeyClient creates a new Valkey client and returns it along with any error encountered.
// It takes a context.Context as input and uses the ClientArgs receiver to access the Host and Port fields.
// The Valkey client is created with the specified Valkey address.
// It then sends a PING command to the Valkey server to check the connection.
// The function returns the Valkey client and any error encountered during the PING command.
func (r *ClientArgs) InitClient(ctx context.Context) error {
	redisAdress := fmt.Sprintf("%s:%s", r.Host, r.Port)

	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{redisAdress}})
	if err != nil {
		return err
	}

	err = client.Do(ctx, client.B().Ping().Build()).Error()
	if err != nil {
		return err
	}

	r.Instance = client

	return nil
}
