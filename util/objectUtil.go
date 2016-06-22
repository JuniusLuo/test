package util

import (
	"encoding/hex"
	"strconv"

	"github.com/nu7hatch/gouuid"
	"golang.org/x/net/context"
)

const (
	// DefaultSeparator is the default separator for misc names
	DefaultSeparator = "."
)

// GenPartName generates the partName
// part name, format: uuid.partNum
func GenPartName(uuid string, partNum int) string {
	return uuid + DefaultSeparator + strconv.Itoa(partNum)
}

// GenRequestID generates a uuid as request id
func GenRequestID() (id string, err error) {
	// generate uuid as request id
	u, err := uuid.NewV4()
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(u[:]), nil
}

// The key type is unexported to prevent collisions with context keys defined in
// other packages.
type key int

// reqIDKey is the context key for the requuid.  Its value of zero is
// arbitrary.  If this package defined other context keys, they would have
// different integer values.
const reqIDKey key = 0

// NewRequestContext returns a new Context carrying requuid.
func NewRequestContext(ctx context.Context, requuid string) context.Context {
	return context.WithValue(ctx, reqIDKey, requuid)
}

// GetReqIDFromContext gets the requuid from ctx.
func GetReqIDFromContext(ctx context.Context) string {
	// ctx.Value returns nil if ctx has no value for the key;
	requuid, _ := ctx.Value(reqIDKey).(string)
	return requuid
}
