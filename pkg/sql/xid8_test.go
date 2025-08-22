package sql

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestXID8_Scan(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected XID8
		wantErr  bool
		errMsg   string
	}{
		// Nil input
		{
			name:    "nil input",
			input:   nil,
			wantErr: true,
			errMsg:  "cannot scan nil value into XID8",
		},

		// Integer types (pgx style)
		{
			name:     "int64 positive",
			input:    int64(12345),
			expected: XID8(12345),
		},
		{
			name:     "int64 zero",
			input:    int64(0),
			expected: XID8(0),
		},
		{
			name:     "int64 max safe value",
			input:    int64(math.MaxInt64),
			expected: XID8(math.MaxInt64),
		},
		{
			name:    "int64 negative",
			input:   int64(-1),
			wantErr: true,
			errMsg:  "cannot convert negative int64",
		},
		{
			name:    "int64 large negative",
			input:   int64(-9223372036854775808), // math.MinInt64
			wantErr: true,
			errMsg:  "cannot convert negative int64",
		},

		// uint64 types
		{
			name:     "uint64 positive",
			input:    uint64(12345),
			expected: XID8(12345),
		},
		{
			name:     "uint64 zero",
			input:    uint64(0),
			expected: XID8(0),
		},
		{
			name:     "uint64 max value",
			input:    uint64(math.MaxUint64),
			expected: XID8(math.MaxUint64),
		},

		// int32 types
		{
			name:     "int32 positive",
			input:    int32(12345),
			expected: XID8(12345),
		},
		{
			name:     "int32 zero",
			input:    int32(0),
			expected: XID8(0),
		},
		{
			name:     "int32 max value",
			input:    int32(math.MaxInt32),
			expected: XID8(math.MaxInt32),
		},
		{
			name:    "int32 negative",
			input:   int32(-1),
			wantErr: true,
			errMsg:  "cannot convert negative int32",
		},

		// uint32 types
		{
			name:     "uint32 positive",
			input:    uint32(12345),
			expected: XID8(12345),
		},
		{
			name:     "uint32 zero",
			input:    uint32(0),
			expected: XID8(0),
		},
		{
			name:     "uint32 max value",
			input:    uint32(math.MaxUint32),
			expected: XID8(math.MaxUint32),
		},
		// String types
		{
			name:     "string positive number",
			input:    "12345",
			expected: XID8(12345),
		},
		{
			name:     "string zero",
			input:    "0",
			expected: XID8(0),
		},
		{
			name:     "string large number",
			input:    "18446744073709551615", // math.MaxUint64
			expected: XID8(math.MaxUint64),
		},
		{
			name:     "string with leading zeros",
			input:    "00012345",
			expected: XID8(12345),
		},
		{
			name:     "empty string",
			input:    "",
			expected: XID8(0),
		},
		{
			name:    "string with negative number",
			input:   "-12345",
			wantErr: true,
			errMsg:  "cannot parse string",
		},
		{
			name:    "string too large for uint64",
			input:   "18446744073709551616", // MaxUint64 + 1
			wantErr: true,
			errMsg:  "cannot parse string",
		},
		// Byte slice types
		{
			name:     "bytes number",
			input:    []byte{51, 55, 57, 51, 50},
			expected: XID8(37932), // "37932" in bytes
		},
		{
			name:     "bytes as string number",
			input:    []byte("12345"),
			expected: XID8(12345),
		},
		{
			name:     "bytes zero",
			input:    []byte("0"),
			expected: XID8(0),
		},
		{
			name:     "bytes large number",
			input:    []byte("18446744073709551615"),
			expected: XID8(math.MaxUint64),
		},
		{
			name:     "empty bytes",
			input:    []byte{},
			expected: XID8(0),
		},
		{
			name:     "empty bytes nil",
			input:    []byte(nil),
			expected: XID8(0),
		},
		{
			name:    "bytes invalid string",
			input:   []byte("abc123"),
			wantErr: true,
			errMsg:  "cannot parse bytes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var x XID8
			err := x.Scan(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, x)
			}
		})
	}
}

func TestXID8_Value(t *testing.T) {
	tests := []struct {
		name     string
		input    XID8
		expected uint64
	}{
		{
			name:     "zero value",
			input:    XID8(0),
			expected: 0,
		},
		{
			name:     "small positive value",
			input:    XID8(12345),
			expected: 12345,
		},
		{
			name:     "max uint64 value",
			input:    XID8(math.MaxUint64),
			expected: math.MaxUint64,
		},
		{
			name:     "real postgres xid8 value",
			input:    XID8(732406),
			expected: 732406,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := tt.input.Value()
			require.NoError(t, err)
			assert.Equal(t, tt.expected, value)
		})
	}
}
