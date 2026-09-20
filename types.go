package xun

import "net/textproto"

// R alias map[string]interface{}, R is the first letter of "Row"
type R map[string]interface{}

// N an numberic value,  R is the first letter of "Numberic"
type N struct {
	Number interface{}
}

// T an datetime value, T is the first letter of "Time"
type T struct {
	Time interface{}
}

// P an Paginator struct, P is the first letter of "Paginator"
type P struct {
	Items        []interface{}          `json:"items"`
	Total        int                    `json:"total"`
	TotalPages   int                    `json:"total_pages"`
	PageSize     int                    `json:"page_size"`
	CurrentPage  int                    `json:"current_page"`
	NextPage     int                    `json:"next_page"`
	PreviousPage int                    `json:"previous_page"`
	LastPage     int                    `json:"last_page"`
	Options      map[string]interface{} `json:"options,omtempty"`
}

// UploadFile deprecated -> gou.UploadFile upload file
type UploadFile struct {
	Name     string
	TempFile string
	Size     int64
	Header   textproto.MIMEHeader
}

// RecordSet 二维紧凑记录集，列名与二维数据行分离，消除单记录哈希桶分配
type RecordSet struct {
	Columns []string        `json:"columns"`
	Rows    [][]interface{} `json:"rows"`
}

// Len 获取行数
func (rs *RecordSet) Len() int {
	if rs == nil {
		return 0
	}
	return len(rs.Rows)
}

// ToR 将 RecordSet 转换为传统的 []R (完全向下兼容)
func (rs *RecordSet) ToR() []R {
	if rs == nil || len(rs.Rows) == 0 {
		return []R{}
	}
	colLen := len(rs.Columns)
	result := make([]R, len(rs.Rows))
	for rowIdx, row := range rs.Rows {
		m := make(R, colLen)
		for colIdx, col := range rs.Columns {
			if colIdx < len(row) {
				m[col] = row[colIdx]
			}
		}
		result[rowIdx] = m
	}
	return result
}

