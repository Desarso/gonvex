package projectbundle

import (
	"encoding/base64"
	"fmt"
)

func decodeFile(encoded string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("invalid base64: %w", err)
	}
	return raw, nil
}

func EncodeFile(content []byte) string {
	return base64.StdEncoding.EncodeToString(content)
}
