package dameng_test

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/yaoapp/xun"
	"github.com/yaoapp/xun/capsule"
	"github.com/yaoapp/xun/dbal/schema"
	_ "github.com/yaoapp/xun/grammar/dameng"
)

func getLiveDSN(t *testing.T) string {
	dsn := os.Getenv("DM_TEST_DSN")
	if dsn == "" {
		host := os.Getenv("DM_TEST_HOST")
		port := os.Getenv("DM_TEST_PORT")
		user := os.Getenv("DM_TEST_USER")
		pass := os.Getenv("DM_TEST_PASS")
		db := os.Getenv("DM_TEST_DB")
		if host != "" && user != "" && pass != "" {
			if port == "" {
				port = "5237"
			}
			if db == "" {
				db = "SUNEED"
			}
			dsn = fmt.Sprintf("dm://%s:%s@%s:%s/%s?autoCommit=true&connectTimeout=5000", user, pass, host, port, db)
		}
	}
	if dsn == "" {
		t.Skip("Skipping live test: set DM_TEST_DSN (or DM_TEST_HOST/USER/PASS) to run live tests against real Dameng instance")
	}
	return dsn
}

// TestLiveDamengSuite 达梦数据库全功能实机 A/B 对照测试套件
func TestLiveDamengSuite(t *testing.T) {
	dsn := getLiveDSN(t)
	t.Logf("Connecting to Dameng server...")

	// -------------------------------------------------------------
	// 阶段 1: 驱动连通性与版本探测
	// -------------------------------------------------------------
	t.Run("Phase1_ConnectionAndVersion", func(t *testing.T) {
		db, err := sql.Open("dm", dsn)
		if err != nil {
			t.Fatalf("Failed to open dm driver: %v", err)
		}
		defer db.Close()

		err = db.Ping()
		if err != nil {
			t.Fatalf("Ping Dameng server failed: %v", err)
		}
		t.Log("✓ Successfully pinged Dameng server")

		// 查询版本信息
		var version string
		err = db.QueryRow("SELECT BANNER FROM V$VERSION").Scan(&version)
		if err != nil {
			err = db.QueryRow("SELECT ID_CODE FROM V$VERSION").Scan(&version)
		}
		assert.NoError(t, err)
		t.Logf("✓ Dameng Version: %s", version)

		// 查询当前 Schema 与当前 User
		var currentUser, currentSchema string
		err = db.QueryRow("SELECT USER, SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL").Scan(&currentUser, &currentSchema)
		assert.NoError(t, err)
		t.Logf("✓ Current User: %s, Current Schema: %s", currentUser, currentSchema)

		// 验证别名 "dameng" 驱动是否可正常通过 sqlx 连接
		sqlxDB, err := sqlx.Open("dameng", dsn)
		assert.NoError(t, err, "sqlx.Open with 'dameng' alias should succeed")
		defer sqlxDB.Close()
		err = sqlxDB.Ping()
		assert.NoError(t, err, "Ping through 'dameng' alias should succeed")
		t.Log("✓ Driver alias 'dameng' verified successfully")
	})

	// -------------------------------------------------------------
	// 阶段 2: DDL 建表与数据类型测试 (含 JSON, 自增, 索引)
	// -------------------------------------------------------------
	testTableName := fmt.Sprintf("test_dm_%d", time.Now().Unix())
	t.Run("Phase2_DDL_And_Types", func(t *testing.T) {
		manager := capsule.New()
		_, err := manager.Add("primary", "dameng", dsn, false)
		if err != nil {
			t.Fatalf("capsule.Add failed: %v", err)
		}
		manager.SetAsGlobal()

		sch := manager.Schema()
		defer func() {
			// 清理测试表
			_ = sch.DropTableIfExists(testTableName)
		}()

		// 创建包含常见业务字段类型、JSON（CLOB）、自增主键、唯一索引的表
		err = sch.CreateTable(testTableName, func(table schema.Blueprint) {
			table.ID("id")
			table.String("username", 64).Unique()
			table.String("email", 128).Null()
			table.Integer("age").SetDefault(18)
			table.Decimal("balance", 10, 2).SetDefault(0.00)
			table.JSON("extra_data").Null() // 重点测试 JSON 映射为 CLOB
			table.String("status").Null()   // 测试未指定长度的 VARCHAR 默认设为 255
			table.Timestamps()
			table.SoftDeletes()
		})
		if err != nil {
			t.Fatalf("CreateTable failed: %v", err)
		}
		t.Logf("✓ Created table %s successfully", testTableName)

		has, err := sch.HasTable(testTableName)
		assert.NoError(t, err)
		assert.True(t, has, "HasTable should find the newly created table")

		// 测试大写传入也能正确判断
		hasUpper, err := sch.HasTable(strings.ToUpper(testTableName))
		assert.NoError(t, err)
		assert.True(t, hasUpper, "HasTable with uppercase name should return true")

		// 获取表元数据
		tbl, err := sch.GetTable(testTableName)
		assert.NoError(t, err)
		if !assert.NotNil(t, tbl) {
			t.Fatalf("GetTable returned nil for table %s", testTableName)
		}
		t.Logf("✓ GetTable retrieved table blueprint successfully")

		// 验证字段类型与列名
		assert.True(t, tbl.HasColumn("id"))
		assert.True(t, tbl.HasColumn("username"))
		assert.True(t, tbl.HasColumn("extra_data"))
		assert.True(t, tbl.HasColumn("deleted_at"))

		// -------------------------------------------------------------
		// 阶段 4: 核心 CRUD 操作与自增 ID 回填验证 (DML)
		// -------------------------------------------------------------
		qb := manager.Query().Table(testTableName)

		// 1. 测试 InsertGetID (重点验证 LastInsertId)
		insertID1, err := qb.InsertGetID(xun.R{
			"username":   "alice",
			"email":      "alice@example.com",
			"age":        25,
			"balance":    123.45,
			"extra_data": "{\"role\": \"admin\"}",
			"created_at": time.Now(),
			"updated_at": time.Now(),
		})
		assert.NoError(t, err, "InsertGetID should succeed")
		assert.Greater(t, insertID1, int64(0), "Generated insertID should be greater than 0")
		t.Logf("✓ InsertGetID inserted record 1 with id: %d", insertID1)

		insertID2, err := qb.InsertGetID(xun.R{
			"username":   "bob",
			"email":      "bob@example.com",
			"age":        30,
			"balance":    500.00,
			"extra_data": "{\"role\": \"editor\"}",
			"created_at": time.Now(),
			"updated_at": time.Now(),
		})
		assert.NoError(t, err)
		assert.Greater(t, insertID2, insertID1, "Subsequent insertID should increment")
		t.Logf("✓ InsertGetID inserted record 2 with id: %d", insertID2)

		// 2. 测试 查询与单引号转义
		row, err := manager.Query().Table(testTableName).Where("username", "alice").First()
		assert.NoError(t, err)
		assert.Equal(t, "alice@example.com", fmt.Sprintf("%v", row.Get("email")))
		t.Log("✓ Select query retrieved inserted data accurately")

		// 3. 测试 分页 (LIMIT / OFFSET)
		list, err := manager.Query().Table(testTableName).OrderBy("id").Limit(1).Offset(1).Get()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(list))
		assert.Equal(t, "bob", fmt.Sprintf("%v", list[0].Get("username")))
		t.Log("✓ Limit and Offset pagination succeeded")

		// 4. 测试 Update
		affected, err := manager.Query().Table(testTableName).Where("id", insertID1).Update(xun.R{
			"balance": 999.99,
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(1), affected)
		t.Log("✓ Update operation succeeded")

		// 5. 测试 Upsert (MERGE INTO)
		upsertData := []xun.R{
			{"username": "alice", "balance": 1000.00, "age": 26},  // 存在，触发更新
			{"username": "charlie", "balance": 88.88, "age": 22}, // 新增
		}
		// uniqueBy 采用 username
		upsertAffected, err := manager.Query().Table(testTableName).Upsert(upsertData, []interface{}{"username"}, []interface{}{"balance", "age"})
		assert.NoError(t, err, "Upsert (MERGE INTO) should succeed")
		t.Logf("✓ Upsert (MERGE INTO) executed successfully, rows affected: %d", upsertAffected)

		// 6. 测试 Delete
		delAffected, err := manager.Query().Table(testTableName).Where("username", "bob").Delete()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), delAffected)
		t.Log("✓ Delete operation succeeded")

		// -------------------------------------------------------------
		// 阶段 5: 事务 Commit & Rollback 验证
		// -------------------------------------------------------------
		tx, err := manager.Primary()
		assert.NoError(t, err)
		sqlTx, err := tx.DB.Begin()
		assert.NoError(t, err)

		_, err = sqlTx.Exec(fmt.Sprintf("INSERT INTO \"%s\" (\"username\", \"age\") VALUES ('tx_user', 99)", testTableName))
		assert.NoError(t, err)

		// 回滚
		err = sqlTx.Rollback()
		assert.NoError(t, err)

		// 验证回滚后数据未入库
		var count int
		err = tx.DB.Get(&count, fmt.Sprintf("SELECT COUNT(*) FROM \"%s\" WHERE \"username\" = 'tx_user'", testTableName))
		assert.NoError(t, err)
		assert.Equal(t, 0, count, "Rolled back record should not exist")
		t.Log("✓ Transaction rollback verified successfully")
	})
}
