# 达梦数据库适配修复任务清单

- [x] 任务 1: 驱动别名与连接层适配
  - [x] 在 `xun/grammar/dameng/dameng.go` 中向 `database/sql` 注册 `"dameng"` 驱动别名（指向 `dm.DmDriver`）
  - [x] 在 `xun/grammar/hooks/hooks.go` 的 `NewDriver` 中补充 `"dameng"` 与 `"dm"` 驱动类型白名单
  - [x] 在 `gou/connector/database/xun.go` 中补充达梦 DSN 生成逻辑（`damengDSN`）

- [x] 任务 2: 完善数据类型映射与字段 DDL 构建
  - [x] 在 `xun/grammar/dameng/dameng.go` 中显式定义 `json`、`jsonb` 为 `CLOB`，`uuid` 为 `VARCHAR`，`enum` 为 `VARCHAR`
  - [x] 在 `dameng.go` 的 `FlipTypes` 中补充反向类型映射
  - [x] 在 `xun/grammar/dameng/builder.go` 的 `SQLAddColumn` 中，为无长度的 `VARCHAR` 赋予默认长度 `255`，`uuid` 设为 `36`，避免达梦报 `VARCHAR必须指定长度`

- [x] 任务 3: 修复 Schema 元数据查询大小写与模式匹配死锁
  - [x] 改造 `schema.go` 中的 `TableExists`，支持多重大小写形态动态匹配（原始名、全大写、全小写）
  - [x] 改造 `schema.go` 中的 `GetColumnListing` 和 `GetIndexListing`，先探测表在达梦数据字典中的真实存储名称，再检索列与索引
  - [x] 将元数据查询条件中的 `OWNER = USER` 增强为兼容当前有效 Schema（`SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')`）

- [x] 任务 4: 修复 DML 插入与自增主键获取
  - [x] 改造 `insert.go` 中的 `CompileInsertGetID` 与 `ProcessInsertGetID`，使用标准 `res.LastInsertId()`，废弃高风险的 `RETURNING` 语法
  - [x] 重写 `insert.go` 中的 `CompileInsertOrIgnore`，废弃非法的 MySQL `insert ignore`，改用符合达梦语法的 `MERGE INTO ... WHEN NOT MATCHED THEN INSERT`

- [x] 任务 5: 优化字符串字面量转义
  - [x] 优化 `xun/grammar/dameng/quoter.go` 中的 `VAL` 方法，单引号转义由 `\'` 修正为 SQL 标准的 `''`

- [x] 任务 6: 本地静态检查与编译验证
  - [x] 编写新增的 `grammar_test.go`
  - [x] 编译并通过所有单元测试（`TestGrammarTypes`, `TestSQLAddColumn`, `TestQuoterVAL`, `TestCompileInsertGetID`, `TestCompileInsertOrIgnore`, `TestParseDSN` 全部 PASS）

- [x] 任务 7: 远程真实达梦（DM8）实例全流程 A/B 测试验证
  - [x] 连通性测试：验证 TCP 连接与驱动别名 `dameng` / `dm` 初始化（PASS）
  - [x] 实例版本探测：识别到 `DM Database Server 64 V8`，Current Schema: `SUNEED`（PASS）
  - [x] DDL 建表验证：包含 String(带默认长度)、Integer、Decimal、JSON(CLOB)、自增主键、索引等（PASS）
  - [x] Schema 逆向元数据：`HasTable`、`GetTable` 成功跨大小写与模式读取列信息（PASS）
  - [x] DML 核心操作：`InsertGetID` 自增回填主键、精准查询、Limit/Offset 分页、Update、Upsert (MERGE INTO) 与 Delete 全链路通过（PASS）
  - [x] 事务隔离与回滚：Begin / Rollback 验证数据回滚完整性（PASS）

- [x] 任务 8: 达梦 Quoter 标识符反引号清洗与原生表达式占位符修复
  - [x] 8.1 修复 `quoter.ID`：清除标识符中的 MySQL 风格反引号，解决双引号套反引号导致达梦报 `-2207 无法解析的成员访问表达式`
  - [x] 8.2 修复 `quoter.Parameterize`：调用 `quoter.Parameter` 区分原生表达式与普通参数，解决 `dbal.Raw` 时间戳导致 `expected 15 arguments, got 14`
  - [x] 8.3 增强 `Wrap` 与 `WrapTable`：补全 `dbal.Name` 与 `string` 分支的别名转义支持
  - [x] 8.4 编写严谨的单元测试：覆盖标识符清洗、复杂 JOIN/WHERE SQL 生成、Raw 表达式与参数混合插入
  - [x] 8.5 运行全量离线单元测试并确保通过

- [x] 任务 10: 修复 Gou 引擎 hasMany 关联查询数据类型归一化与多数据库兼容（方案一）
  - [x] 10.1 在 `gou/model/stack.go` 中实现安全高效的 `normalizeKey`，消除 `map[interface{}]` 跨库整型/浮点/字符串类型漂移
  - [x] 10.2 在 `runHasMany` 中应用 `normalizeKey` 索引 `fmtRowMap` 与主表 `id` 回填匹配
  - [x] 10.3 在 `runHasMany`、`run`、`paginate` 中增加 `builder.ColumnMap` 大小写容错兜底
  - [x] 10.4 优化 `prevRows` 父级主表定位，同时解决同级多 `hasMany` 关系穿透问题
  - [x] 10.5 编写严谨的单元测试覆盖不同数值类型匹配与关联回填
  - [x] 10.6 验证现有单元测试 100% 通过

## 修复成果与阶段总结
已全面扫清达梦数据库在语法生成、驱动注册、DDL 构建、Schema 逆向与 DML 插入方面的所有已知缺陷，本地离线单元测试与远端真实达梦（DM8）实机集成测试全部 100% 通过！

