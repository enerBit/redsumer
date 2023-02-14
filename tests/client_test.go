package gorgp

import (
	"fmt"
	"testing"

	"github.com/enerBit/redsumer/pkg/client"
)

func TestClient(t *testing.T) {

	client, _ := client.NewRedisClient("localhost", 6379, 0)
	clientTypeString := fmt.Sprint(client)

	if clientTypeString != "Redis<localhost:6379 db:0>" {
		t.Error(clientTypeString)
	}

}
