package sql

import (
	"strings"
)

const (
	backtick = "`"
)

func backticks(s string) string {
	if strings.HasPrefix(s, backtick) && strings.HasSuffix(s, backtick) {
		return s
	}

	return backtick + s + backtick
}
