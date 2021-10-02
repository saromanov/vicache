package vicache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCache(t *testing.T) {
	_, err := New(10, 512)
	assert.Error(t, err)
	n, err := New(512, 10)
	assert.NoError(t, err)
	n.Set([]byte("test"), []byte("key"))
	res := n.Get(nil, []byte("test"))
	assert.Equal(t, "key", string(res))
	n.Del([]byte("test"))
	res = n.Get(nil, []byte("test"))
	assert.Empty(t, res)
}
