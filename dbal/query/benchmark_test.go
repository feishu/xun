package query

import (
	"reflect"
	"testing"
)

// reflectGetValue 原实现的反射逻辑，用于基准对照
func reflectGetValue(src interface{}) interface{} {
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

func BenchmarkGetValue_Reflect(b *testing.B) {
	var v interface{} = "hello world"
	ptr := &v

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = reflectGetValue(ptr)
	}
}

func BenchmarkGetValue_Optimized(b *testing.B) {
	builder := &Builder{}
	var v interface{} = "hello world"
	ptr := &v

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = builder.getValue(ptr)
	}
}

func BenchmarkGetValue_Bytes_Reflect(b *testing.B) {
	var v interface{} = []byte("some long text column value from database rows")
	ptr := &v

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = reflectGetValue(ptr)
	}
}

func BenchmarkGetValue_Bytes_Optimized(b *testing.B) {
	builder := &Builder{}
	var v interface{} = []byte("some long text column value from database rows")
	ptr := &v

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = builder.getValue(ptr)
	}
}
