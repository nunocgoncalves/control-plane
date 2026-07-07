package identity

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateAPIKey(t *testing.T) {
	full, prefix, hash, err := GenerateAPIKey()
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(full, "cp-"), "key should be cp- prefixed, got %q", full)
	assert.Equal(t, hash, HashAPIKey(full), "returned hash should match HashAPIKey(full)")
	assert.NotEqual(t, full, hash, "hash must not equal the full key")
	assert.Equal(t, prefix, full[:len(prefix)], "prefix should be the head of the full key")
	assert.GreaterOrEqual(t, len(full), len("cp-")+16, "key should have ample entropy")

	// Two generations must differ.
	full2, _, hash2, err := GenerateAPIKey()
	require.NoError(t, err)
	assert.NotEqual(t, full, full2)
	assert.NotEqual(t, hash, hash2)
}

func TestParseBearer(t *testing.T) {
	cases := []struct {
		name   string
		header string
		want   string
		ok     bool
	}{
		{"standard", "Bearer cp-ABCD", "cp-ABCD", true},
		{"lowercase scheme", "bearer cp-ABCD", "cp-ABCD", true},
		{"whitespace", "Bearer   cp-XYZ  ", "cp-XYZ", true},
		{"missing", "", "", false},
		{"no scheme", "cp-ABCD", "", false},
		{"basic", "Basic dXNlcjpwYXNz", "", false},
		{"bearer empty", "Bearer ", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseBearer(tc.header)
			assert.Equal(t, tc.ok, ok)
			if ok {
				assert.Equal(t, tc.want, got)
			}
		})
	}
}

func TestValidScope(t *testing.T) {
	assert.True(t, ValidScope(ScopeAdmin))
	assert.True(t, ValidScope(ScopeToken))
	assert.True(t, ValidScope(ScopeGateway))
	assert.False(t, ValidScope("superuser"))
	assert.False(t, ValidScope(""))
}
