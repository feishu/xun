package capsule

import (
	"fmt"
	"sync/atomic"
)

// RandPrimary select primary connection using atomic round-robin (zero-allocation & perfectly balanced)
func (pool *Pool) RandPrimary() (*Connection, error) {
	length := len(pool.Primary)
	if length == 0 {
		return nil, fmt.Errorf("the primary connection was empty")
	}
	if length == 1 {
		return pool.Primary[0], nil
	}

	idx := atomic.AddUint64(&pool.primaryIdx, 1) - 1
	return pool.Primary[idx%uint64(length)], nil
}

// RandReadOnly select readonly connection using atomic round-robin (zero-allocation & perfectly balanced)
func (pool *Pool) RandReadOnly() (*Connection, error) {
	length := len(pool.Readonly)
	if length == 0 {
		return pool.RandPrimary()
	}
	if length == 1 {
		return pool.Readonly[0], nil
	}

	idx := atomic.AddUint64(&pool.readonlyIdx, 1) - 1
	return pool.Readonly[idx%uint64(length)], nil
}
