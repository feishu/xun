package query

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yaoapp/xun"
)

func TestSupportIsQueryable(t *testing.T) {
	NewTableForWhereTest()
	qb := getTestBuilder()
	builder := getTestBuilderInstance()
	var empty interface{} = nil
	assert.False(t, builder.isQueryable(empty), "The nil interface{} should not be queryable")
	assert.False(t, builder.isQueryable(func() {}), "The func(){} should not be queryable")
	assert.True(t, builder.isQueryable(func(qb Query) {}), "The func(qb Query) {} should be queryable")
	assert.True(t, builder.isQueryable(builder), "The builder instance should be queryable")
	assert.True(t, builder.isQueryable(qb), "The Query interface should be queryable")
}

func TestSupportIsBoolean(t *testing.T) {
	builder := getTestBuilderInstance()
	assert.True(t, builder.isBoolean("and"), "The return value should be true")
	assert.True(t, builder.isBoolean("or"), "The return value should be true")
	assert.False(t, builder.isBoolean("not"), "The return value should be false")
}

func TestSupportGetValue(t *testing.T) {
	builder := &Builder{}

	// nil
	assert.Nil(t, builder.getValue(nil))

	// *interface{}
	var i interface{} = "hello"
	assert.Equal(t, "hello", builder.getValue(&i))

	// *string
	str := "world"
	assert.Equal(t, "world", builder.getValue(&str))

	// *[]byte
	b := []byte("binary_bytes")
	assert.Equal(t, "binary_bytes", builder.getValue(&b))

	// *int64
	var num int64 = 1024
	assert.Equal(t, int64(1024), builder.getValue(&num))

	// *bool
	var boolean = true
	assert.Equal(t, true, builder.getValue(&boolean))

	// string
	assert.Equal(t, "direct_str", builder.getValue("direct_str"))
}

func TestRecordSetToR(t *testing.T) {
	rs := &xun.RecordSet{
		Columns: []string{"id", "name", "email"},
		Rows: [][]interface{}{
			{1, "Alice", "alice@example.com"},
			{2, "Bob", "bob@example.com"},
		},
	}

	assert.Equal(t, 2, rs.Len())
	rList := rs.ToR()
	assert.Equal(t, 2, len(rList))
	assert.Equal(t, 1, rList[0]["id"])
	assert.Equal(t, "Alice", rList[0]["name"])
	assert.Equal(t, "alice@example.com", rList[0]["email"])
	assert.Equal(t, 2, rList[1]["id"])
	assert.Equal(t, "Bob", rList[1]["name"])
	assert.Equal(t, "bob@example.com", rList[1]["email"])
}

func TestScanContextCancellationDuringIteration(t *testing.T) {
	NewTableForQueryTest()
	qb := getTestBuilder()

	// Normal query gets rows
	res, err := qb.Table("table_test_query").Get()
	assert.NoError(t, err)
	assert.Greater(t, len(res), 0)

	// Context with cancel upfront
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = qb.WithContext(ctx).Table("table_test_query").Get()
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)

	// Test RecordSetScan context cancellation
	_, err = qb.WithContext(ctx).Table("table_test_query").GetRecordSet()
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}


