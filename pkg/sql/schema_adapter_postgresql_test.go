package sql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultInsertMarkers(t *testing.T) {
	testCases := []struct {
		Count          int
		ExpectedOutput string
	}{
		{
			Count:          0,
			ExpectedOutput: "",
		},
		{
			Count:          1,
			ExpectedOutput: "($1,$2,$3,pg_current_xact_id())",
		},
		{
			Count:          2,
			ExpectedOutput: "($1,$2,$3,pg_current_xact_id()),($4,$5,$6,pg_current_xact_id())",
		},
		{
			Count: 5,
			ExpectedOutput: "($1,$2,$3,pg_current_xact_id())," +
				"($4,$5,$6,pg_current_xact_id())," +
				"($7,$8,$9,pg_current_xact_id())," +
				"($10,$11,$12,pg_current_xact_id())," +
				"($13,$14,$15,pg_current_xact_id())",
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d", tc.Count), func(t *testing.T) {
			output := defaultInsertMarkers(tc.Count)
			assert.Equal(t, tc.ExpectedOutput, output)
		})
	}
}
