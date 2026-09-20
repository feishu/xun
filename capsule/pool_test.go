package capsule

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yaoapp/xun/dbal"
)

func TestPoolRoundRobin(t *testing.T) {
	conn1 := &Connection{Config: &dbal.Config{Name: "c1"}}
	conn2 := &Connection{Config: &dbal.Config{Name: "c2"}}
	conn3 := &Connection{Config: &dbal.Config{Name: "c3"}}

	pool := &Pool{
		Primary:  []*Connection{conn1, conn2, conn3},
		Readonly: []*Connection{conn1, conn2},
	}

	// Test primary round-robin distribution
	for i := 0; i < 9; i++ {
		c, err := pool.RandPrimary()
		assert.NoError(t, err)
		expected := pool.Primary[i%3]
		assert.Equal(t, expected.Config.Name, c.Config.Name, "Primary round-robin mismatch at iteration %d", i)
	}

	// Test readonly round-robin distribution
	for i := 0; i < 6; i++ {
		c, err := pool.RandReadOnly()
		assert.NoError(t, err)
		expected := pool.Readonly[i%2]
		assert.Equal(t, expected.Config.Name, c.Config.Name, "Readonly round-robin mismatch at iteration %d", i)
	}

	// Test single node fast path
	singlePool := &Pool{Primary: []*Connection{conn1}}
	c, err := singlePool.RandPrimary()
	assert.NoError(t, err)
	assert.Equal(t, "c1", c.Config.Name)
}

func BenchmarkPoolRoundRobin(b *testing.B) {
	conn1 := &Connection{Config: &dbal.Config{Name: "c1"}}
	conn2 := &Connection{Config: &dbal.Config{Name: "c2"}}
	pool := &Pool{
		Primary: []*Connection{conn1, conn2},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = pool.RandPrimary()
	}
}
