package query

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/yaoapp/xun"
)

// reflectGetValue 旧版本的反射行扫描逻辑
func oldReflectGetValue(src interface{}) interface{} {
	value := src
	if reflect.TypeOf(src).Kind() == reflect.Ptr {
		value = reflect.Indirect(reflect.ValueOf(src)).Interface()
	}
	switch v := value.(type) {
	case []byte:
		return string(v)
	default:
		return value
	}
}

// 模拟旧版的 mapScan（无预分配 + 反射提取）
func oldMapScan(rows *sql.Rows, columns []string) ([]xun.R, error) {
	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(interface{})
	}
	res := []xun.R{}
	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return nil, err
		}
		dest := xun.R{} // 未预分配
		for i, column := range columns {
			dest[column] = oldReflectGetValue(values[i]) // 反射
		}
		res = append(res, dest)
	}
	return res, nil
}

// 优化后的 mapScan（预分配 + 指针快速断言）
func optimizedMapScan(builder *Builder, rows *sql.Rows, columns []string) ([]xun.R, error) {
	values := builder.makeMapValues(len(columns))
	res := []xun.R{}
	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return nil, err
		}
		dest := make(xun.R, len(columns)) // 预分配容量
		for i, column := range columns {
			dest[column] = builder.getValue(values[i]) // 快速断言
		}
		res = append(res, dest)
	}
	return res, nil
}

// ==========================================
// 证据 1: 真实 SQLite 查询对比 (Prepare+Close vs Direct Query)
// ==========================================
func BenchmarkE2E_Query_PrepareVsDirect(b *testing.B) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, score INTEGER)`)
	if err != nil {
		b.Fatal(err)
	}
	for i := 1; i <= 100; i++ {
		_, _ = db.Exec(`INSERT INTO users (id, name, email, score) VALUES (?, ?, ?, ?)`,
			i, fmt.Sprintf("user_%d", i), fmt.Sprintf("user_%d@test.com", i), i*10)
	}

	b.Run("Old_Prepare_And_Close", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			stmt, err := db.Prepare("SELECT id, name, email, score FROM users WHERE id = ?")
			if err != nil {
				b.Fatal(err)
			}
			rows, err := stmt.Query((i % 100) + 1)
			if err != nil {
				stmt.Close()
				b.Fatal(err)
			}
			for rows.Next() {
			}
			rows.Close()
			stmt.Close()
		}
	})

	b.Run("Optimized_Direct_Query", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query("SELECT id, name, email, score FROM users WHERE id = ?", (i%100)+1)
			if err != nil {
				b.Fatal(err)
			}
			for rows.Next() {
			}
			rows.Close()
		}
	})
}

// ==========================================
// 证据 2: 真实 1000 行扫描性能对比 (Old reflect vs Optimized Assertion)
// ==========================================
func BenchmarkE2E_BatchScan_1000Rows(b *testing.B) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		order_no TEXT,
		user_id INTEGER,
		amount REAL,
		status TEXT,
		created_at TEXT,
		remark TEXT
	)`)
	if err != nil {
		b.Fatal(err)
	}

	tx, _ := db.Begin()
	for i := 1; i <= 1000; i++ {
		_, _ = tx.Exec(`INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?)`,
			i, fmt.Sprintf("ORD_2026_%06d", i), 1000+i, 99.9, "paid", "2026-09-19 00:00:00", "some order remark text")
	}
	_ = tx.Commit()

	builder := &Builder{}

	b.Run("Old_MapScan_Reflect_1000Rows", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query("SELECT * FROM orders")
			if err != nil {
				b.Fatal(err)
			}
			cols, _ := rows.Columns()
			res, err := oldMapScan(rows, cols)
			if err != nil {
				b.Fatal(err)
			}
			if len(res) != 1000 {
				b.Fatalf("expected 1000 rows, got %d", len(res))
			}
			rows.Close()
		}
	})

	b.Run("Optimized_MapScan_Fast_1000Rows", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query("SELECT * FROM orders")
			if err != nil {
				b.Fatal(err)
			}
			cols, _ := rows.Columns()
			res, err := optimizedMapScan(builder, rows, cols)
			if err != nil {
				b.Fatal(err)
			}
			if len(res) != 1000 {
				b.Fatalf("expected 1000 rows, got %d", len(res))
			}
			rows.Close()
		}
	})
}
