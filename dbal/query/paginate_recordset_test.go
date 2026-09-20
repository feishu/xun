package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPaginateRecordSetBasic(t *testing.T) {
	NewTableForPaginateTest()
	qb := getTestBuilder()
	p := qb.Table("table_test_paginate").
		Where("email", "like", "%@yao.run").
		Select("id", "name", "email", "vote", "score", "status").
		OrderBy("vote", "desc").
		OrderBy("score").
		MustPaginateRecordSet(2, 1)

	// Checking paginator metadata
	assert.Equal(t, 4, p.Total, "The records total should be 4")
	assert.Equal(t, 2, p.PageSize, "The page size should be 2")
	assert.Equal(t, 2, p.TotalPages, "The total pages should be 2")
	assert.Equal(t, 1, p.CurrentPage, "The current page should be 1")
	assert.Equal(t, 2, p.NextPage, "The next page should be 2")
	assert.Equal(t, -1, p.PreviousPage, "The previous page should be -1")

	// Checking record set content
	assert.NotNil(t, p.RecordSet)
	assert.Equal(t, 2, p.RecordSet.Len())
	assert.Equal(t, 6, len(p.RecordSet.Columns))

	// Rows should match expected values
	rows := p.RecordSet.ToR()
	assert.Equal(t, 2, len(rows))
	assert.Equal(t, int64(3), rows[0].Get("id"))
	assert.Equal(t, int64(1), rows[1].Get("id"))

	// Page 2
	p2 := qb.MustPaginateRecordSet(2, 2)
	assert.Equal(t, 4, p2.Total)
	assert.Equal(t, 2, p2.CurrentPage)
	assert.Equal(t, -1, p2.NextPage)
	assert.Equal(t, 1, p2.PreviousPage)
	assert.Equal(t, 2, p2.RecordSet.Len())

	rows2 := p2.RecordSet.ToR()
	assert.Equal(t, int64(4), rows2[0].Get("id"))
	assert.Equal(t, int64(2), rows2[1].Get("id"))
}
