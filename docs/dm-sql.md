# 达梦数据库 DM SQL 语法完全指南

本指南基于官方文档及技术实践，全面覆盖达梦数据库（DM）SQL 语法体系，重点标注与 MySQL/PostgreSQL 的差异及达梦独有特性。

---

## 一、数据定义语言（DDL）

### 1.1 数据库与模式管理

**创建数据库（DM 特有）**
```sql
-- DM 支持在创建时指定编码、日志文件等
CREATE DATABASE "DAMENG" 
LOGFILE '/dm/dmdbms/DAMENG01.log' SIZE 256 MB
DATAFILE '/dm/dmdbms/DAMENG.DBF' SIZE 1024 MB
AUTOEXTEND ON NEXT 256 MB MAXSIZE 10240 MB
CHARSET UTF8;  -- 指定字符集
```

**模式（SCHEMA）管理**
```sql
-- 创建模式（必须指定用户名）
CREATE SCHEMA "CRM" AUTHORIZATION "SYSDBA";

-- 切换当前模式
SET SCHEMA "CRM";

-- 删除模式（级联删除所有对象）
DROP SCHEMA "CRM" CASCADE;
```
**与 MySQL 差异**：DM 强制 SCHEMA 概念，用户与 SCHEMA 可一对多；MySQL 的 DATABASE 等价于 DM 的 SCHEMA 

---

### 1.2 表定义语法

**基础建表（含 DM 特有元素）**
```sql
CREATE TABLE "SYSDBA"."orders" (
    -- 1. 自增列：IDENTITY 或 AUTO_INCREMENT（兼容模式）
    order_id BIGINT IDENTITY(1, 1) PRIMARY KEY,
    
    -- 2. 数据类型：VARCHAR2、NUMBER、CLOB/BLOB
    customer_name VARCHAR2(200) NOT NULL,
    order_amount NUMBER(18, 2) DEFAULT 0.00,
    order_status VARCHAR2(20) DEFAULT 'pending',
    
    -- 3. 大对象字段
    order_detail CLOB,  -- 替代 MySQL LONGTEXT
    invoice_pdf BLOB,   -- 替代 MySQL LONGBLOB
    
    -- 4. 虚拟列（DM 特有）
    order_year INT GENERATED ALWAYS AS (YEAR(create_time)),
    
    -- 5. 行迁移/行链接控制
    pctfree 10          -- 预留 10% 空间用于更新
) 
TABLESPACE "MAIN"      -- 指定表空间
STORAGE (
    INITIAL 64K,       -- 初始分配 64K
    NEXT 64K,          -- 每次扩展 64K
    MINEXTENTS 1,      -- 最小 1 个区
    MAXEXTENTS UNLIMITED
);
```

**临时表（DM 特有语法）**
```sql
-- 事务级临时表：事务结束自动清空
CREATE GLOBAL TEMPORARY TABLE "temp_sales" (
    product_id INT,
    daily_amount NUMBER(10, 2)
) ON COMMIT DELETE ROWS;

-- 会话级临时表：会话结束清空
CREATE GLOBAL TEMPORARY TABLE "temp_session" (
    user_id INT,
    temp_value VARCHAR2(100)
) ON COMMIT PRESERVE ROWS;
```

**分区表（DM 高级特性）**
```sql
-- 范围分区
CREATE TABLE "sales_fact" (
    sale_id INT,
    sale_date DATE,
    amount NUMBER(10, 2)
)
PARTITION BY RANGE (sale_date) (
    PARTITION p2023 VALUES LESS THAN (DATE '2024-01-01'),
    PARTITION p2024 VALUES LESS THAN (DATE '2025-01-01'),
    PARTITION pmax VALUES LESS THAN (MAXVALUE)
);

-- 列表分区
PARTITION BY LIST (region) (
    PARTITION p_east VALUES ('北京', '上海', '广州'),
    PARTITION p_west VALUES ('成都', '西安', '重庆')
);

-- 哈希分区
PARTITION BY HASH (customer_id) PARTITIONS 16;
```

---

### 1.3 修改表结构

```sql
-- 添加列
ALTER TABLE "SYSDBA".orders ADD COLUMN "remark" VARCHAR2(500);

-- 修改列类型（DM 特有 MODIFY COLUMN）
ALTER TABLE "SYSDBA".orders MODIFY COLUMN "order_status" VARCHAR2(50);

-- 删除列
ALTER TABLE "SYSDBA".orders DROP COLUMN "remark";

-- 重命名列（DM 特有）
ALTER TABLE "SYSDBA".orders RENAME COLUMN "order_amount" TO "total_amount";

-- 添加约束
ALTER TABLE "SYSDBA".orders ADD CONSTRAINT "chk_status" 
CHECK (order_status IN ('pending', 'paid', 'cancelled'));

-- 添加外键
ALTER TABLE "SYSDBA".order_items 
ADD CONSTRAINT "fk_order" FOREIGN KEY (order_id) 
REFERENCES "SYSDBA".orders(order_id);
```

---

### 1.4 索引管理

```sql
-- 创建 B 树索引
CREATE INDEX "idx_customer_name" ON "SYSDBA".orders(customer_name);

-- 创建唯一索引
CREATE UNIQUE INDEX "idx_order_no" ON "SYSDBA".orders(order_no);

-- 创建位图索引（DM 特有，适合低基数列）
CREATE BITMAP INDEX "idx_order_status" ON "SYSDBA".orders(order_status);

-- 创建函数索引
CREATE INDEX "idx_upper_name" ON "SYSDBA".orders(UPPER(customer_name));

-- 创建全文索引
CREATE CONTEXT INDEX "idx_product_desc" ON "SYSDBA".products(product_desc)
LEXER CHINESE_LEXER;  -- 指定中文分词器

-- 索引重组（DM 特有）
ALTER INDEX "idx_customer_name" REBUILD;
```

---

### 1.5 视图与同义词

**视图（支持物化视图）**
```sql
-- 普通视图
CREATE OR REPLACE VIEW "v_order_summary" AS
SELECT order_id, SUM(amount) AS total_amount
FROM order_items
GROUP BY order_id;

-- 物化视图（DM 特有，支持查询重写）
CREATE MATERIALIZED VIEW "mv_sales_monthly"
BUILD IMMEDIATE           -- 立即构建
REFRESH COMPLETE ON DEMAND  -- 手动完全刷新
ENABLE QUERY REWRITE      -- 启用查询重写
AS
SELECT DATE_TRUNC('MONTH', sale_date) AS month, SUM(amount) AS total
FROM sales_fact
GROUP BY DATE_TRUNC('MONTH', sale_date);
```

**同义词（DM 特有，兼容 Oracle）**
```sql
-- 创建同义词，简化跨模式访问
CREATE SYNONYM "orders" FOR "SYSDBA"."orders";

-- 创建公共同义词
CREATE PUBLIC SYNONYM "products" FOR "PROD"."products";

-- 删除同义词
DROP SYNONYM "orders";
```

---

### 1.6 序列管理

**DM 序列（替代 MySQL AUTO_INCREMENT）**
```sql
-- 创建序列
CREATE SEQUENCE "seq_order_id"
START WITH 1          -- 起始值
INCREMENT BY 1        -- 步长
MAXVALUE 9999999999   -- 最大值
NOCYCLE               -- 不循环
CACHE 100;            -- 缓存 100 个值

-- 使用序列
INSERT INTO orders (order_id, name) VALUES (seq_order_id.NEXTVAL, '测试订单');

-- 查看当前值
SELECT seq_order_id.CURRVAL FROM dual;

-- 修改序列
ALTER SEQUENCE "seq_order_id" INCREMENT BY 10;

-- 删除序列
DROP SEQUENCE "seq_order_id";
```

---

## 二、数据操作语言（DML）

### 2.1 SELECT 查询

**基础查询（含 DM 特有语法）**
```sql
-- 1. 标准查询
SELECT * FROM "SYSDBA".orders WHERE order_status = 'paid';

-- 2. DM 特有：ROWNUM 伪列（类似 Oracle）
SELECT * FROM orders WHERE ROWNUM <= 10;  -- 取前10行

-- 3. DM 特有：CONNECT BY 层次查询
SELECT employee_id, manager_id, employee_name, LEVEL
FROM employees
START WITH manager_id IS NULL  -- 根节点条件
CONNECT BY PRIOR employee_id = manager_id;  -- 父子关系

-- 4. DM 特有：PIVOT/UNPIVOT 行列转换
SELECT * FROM sales
PIVOT (
    SUM(amount) FOR quarter IN ('Q1' AS q1, 'Q2' AS q2, 'Q3' AS q3, 'Q4' AS q4)
);

-- 5. DM 支持：窗口函数
SELECT 
    order_id,
    customer_name,
    order_amount,
    ROW_NUMBER() OVER (PARTITION BY customer_name ORDER BY order_amount DESC) AS rank_num,
    SUM(order_amount) OVER (PARTITION BY customer_name) AS total_amount
FROM "SYSDBA".orders;

-- 6. DM 特有：查询结果集缓存
SELECT /*+ RESULT_CACHE */ * FROM large_table WHERE id = 100;
```

**分页查询（多语法支持）**
```sql
-- 方法1：LIMIT OFFSET（DM 兼容模式）
SELECT * FROM orders LIMIT 20, 10;  -- MySQL 风格

-- 方法2：标准 SQL:2008（推荐）
SELECT * FROM orders 
ORDER BY order_id 
OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY;

-- 方法3：ROWNUM 嵌套（Oracle 风格）
SELECT * FROM (
    SELECT t.*, ROWNUM rn FROM orders t
) WHERE rn BETWEEN 21 AND 30;
```

---

### 2.2 INSERT 插入

**标准插入**
```sql
-- 单行插入
INSERT INTO "SYSDBA".orders (order_id, customer_name) 
VALUES (1, '客户A');

-- 批量插入
INSERT INTO "SYSDBA".orders (order_id, customer_name) 
VALUES (2, '客户B'), (3, '客户C'), (4, '客户D');
```

**插入并返回（DM 特有）**
```sql
-- 插入后返回自增值
INSERT INTO "SYSDBA".orders (customer_name) 
VALUES ('客户E') 
RETURNING order_id INTO :new_id;  -- 返回到绑定变量

-- 从查询结果插入
INSERT INTO "SYSDBA".order_archive (order_id, create_time)
SELECT order_id, SYSDATE FROM "SYSDBA".orders WHERE status = 'cancelled';
```

**DM 特有：MERGE INTO（替代 MySQL ON DUPLICATE KEY）**
```sql
MERGE INTO "SYSDBA".customer t
USING (SELECT 1001 AS cust_id, '张三' AS name FROM dual) s
ON (t.cust_id = s.cust_id)
WHEN MATCHED THEN 
    UPDATE SET t.name = s.name, t.update_time = SYSDATE
WHEN NOT MATCHED THEN 
    INSERT (cust_id, name) VALUES (s.cust_id, s.name);
```

---

### 2.3 UPDATE 更新

```sql
-- 标准更新
UPDATE "SYSDBA".orders 
SET order_status = 'paid', paid_time = SYSDATE 
WHERE order_id = 100;

-- 关联更新（DM 支持多表关联 UPDATE）
UPDATE "SYSDBA".orders o
SET o.customer_level = c.level
FROM "SYSDBA".customer c
WHERE o.customer_id = c.id;

-- 使用子查询更新
UPDATE "SYSDBA".orders 
SET (customer_name, region) = (
    SELECT name, region FROM customer WHERE id = orders.customer_id
)
WHERE order_id = 200;
```

---

### 2.4 DELETE 删除

```sql
-- 标准删除
DELETE FROM "SYSDBA".orders WHERE order_status = 'cancelled';

-- 关联删除（DM 特有语法）
DELETE FROM "SYSDBA".orders o
WHERE EXISTS (
    SELECT 1 FROM customer c WHERE c.id = o.customer_id AND c.status = 'inactive'
);

-- 快速清空（DM 特有，类似 Oracle）
TRUNCATE TABLE "SYSDBA".temp_orders;
```

---

## 三、数据控制语言（DCL）

### 3.1 权限管理

```sql
-- 创建用户（DM 特有语法）
CREATE USER "crm_user" IDENTIFIED BY "DAmeng123456" 
DEFAULT TABLESPACE "MAIN"
TEMPORARY TABLESPACE "TEMP";

-- 授权
GRANT CREATE TABLE, CREATE VIEW TO "crm_user";
GRANT SELECT, INSERT, UPDATE ON "SYSDBA".orders TO "crm_user";

-- 角色管理（推荐）
CREATE ROLE "role_crm_admin";
GRANT ALL PRIVILEGES ON "CRM".* TO "role_crm_admin";
GRANT "role_crm_admin" TO "crm_user";

-- 回收权限
REVOKE INSERT ON "SYSDBA".orders FROM "crm_user";

-- 删除用户
DROP USER "crm_user" CASCADE;
```

---

## 四、事务控制语言（TCL）

```sql
-- 开启事务
BEGIN;  -- 或 START TRANSACTION

-- 设置事务属性（DM 特有）
SET TRANSACTION 
    ISOLATION LEVEL READ COMMITTED  -- 隔离级别
    READ ONLY;                     -- 只读事务

-- 保存点
SAVEPOINT sp1;

-- 回滚到保存点
ROLLBACK TO SAVEPOINT sp1;

-- 提交事务
COMMIT;

-- 回滚整个事务
ROLLBACK;
```

**DM 事务特性**：
- 默认隔离级别：READ COMMITTED 
- 支持分布式事务（XA 协议）
- 支持自治事务（PRAGMA AUTONOMOUS_TRANSACTION）

---

## 五、达梦特有高级语法

### 5.1 存储过程（PL/SQL 兼容）

```sql
CREATE OR REPLACE PROCEDURE "PROC_PROCESS_ORDER"(
    p_order_id IN INT,
    p_status OUT VARCHAR2,
    p_msg OUT VARCHAR2
)
AS
    -- 变量声明
    v_count INT;
    v_customer_name VARCHAR2(200);
    
    -- 自定义异常
    e_order_not_found EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_order_not_found, -20001);
BEGIN
    -- 查询订单
    SELECT COUNT(*) INTO v_count FROM orders WHERE order_id = p_order_id;
    
    IF v_count = 0 THEN
        RAISE_APPLICATION_ERROR(-20001, '订单不存在');
    END IF;
    
    -- 获取客户名
    SELECT customer_name INTO v_customer_name 
    FROM orders WHERE order_id = p_order_id;
    
    -- 业务逻辑处理
    BEGIN
        UPDATE orders SET process_flag = 'Y' WHERE order_id = p_order_id;
        p_status := 'SUCCESS';
        p_msg := '订单处理成功: ' || v_customer_name;
    EXCEPTION
        WHEN OTHERS THEN
            p_status := 'ERROR';
            p_msg := '处理失败: ' || SQLERRM;
    END;
    
    COMMIT;
    
EXCEPTION
    WHEN e_order_not_found THEN
        p_status := 'ERROR';
        p_msg := p_msg || ' - 异常代码: ' || SQLCODE;
        ROLLBACK;
    WHEN OTHERS THEN
        p_status := 'ERROR';
        p_msg := '未知错误: ' || SQLERRM;
        ROLLBACK;
END;
/
```

**调用存储过程**
```sql
DECLARE
    v_status VARCHAR2(20);
    v_msg VARCHAR2(500);
BEGIN
    PROC_PROCESS_ORDER(1001, v_status, v_msg);
    DBMS_OUTPUT.PUT_LINE('状态: ' || v_status || ', 消息: ' || v_msg);
END;
/
```

---

### 5.2 自定义函数

```sql
CREATE OR REPLACE FUNCTION "FUNC_CALC_DISCOUNT"(
    p_amount IN NUMBER,
    p_level IN VARCHAR2
) RETURN NUMBER
DETERMINISTIC  -- 确定性函数标记（DM 优化提示）
AS
    v_discount_rate NUMBER := 1.0;
BEGIN
    CASE p_level
        WHEN 'VIP' THEN v_discount_rate := 0.8;
        WHEN 'GOLD' THEN v_discount_rate := 0.85;
        WHEN 'SILVER' THEN v_discount_rate := 0.9;
        ELSE v_discount_rate := 1.0;
    END CASE;
    
    RETURN p_amount * v_discount_rate;
END;
/

-- 在 SQL 中调用函数
SELECT order_id, FUNC_CALC_DISCOUNT(order_amount, customer_level) 
FROM orders;
```

---

### 5.3 触发器

```sql
-- DM 特有：行级触发器
CREATE OR REPLACE TRIGGER "TRG_BEFORE_ORDER_INSERT"
BEFORE INSERT ON "SYSDBA".orders
FOR EACH ROW
BEGIN
    -- 自动生成订单号
    :NEW.order_no := 'ORD' || TO_CHAR(SYSDATE, 'YYYYMMDD') || seq_order_id.NEXTVAL;
    
    -- 记录创建时间
    :NEW.create_time := SYSDATE;
    
    -- 数据验证
    IF :NEW.order_amount < 0 THEN
        RAISE_APPLICATION_ERROR(-20002, '订单金额不能为负');
    END IF;
END;
/

-- DM 特有：语句级触发器
CREATE OR REPLACE TRIGGER "TRG_AFTER_ORDER_UPDATE"
AFTER UPDATE OF order_status ON "SYSDBA".orders
BEGIN
    -- 记录操作日志
    INSERT INTO operation_log (table_name, operation_type, op_time)
    VALUES ('orders', 'UPDATE', SYSDATE);
END;
/

-- 查看触发器状态
SELECT * FROM SYSOBJECTS WHERE TYPE$ = 'SCHTRG';
```

---

### 5.4 包（Package，DM 特有）

```sql
-- 创建包规范
CREATE OR REPLACE PACKAGE "PKG_ORDER_MANAGE" AS
    -- 公共变量
    g_max_amount NUMBER := 1000000;
    
    -- 公共过程
    PROCEDURE add_order(p_customer_id INT, p_amount NUMBER);
    
    -- 公共函数
    FUNCTION get_order_count(p_customer_id INT) RETURN INT;
    
    -- 异常定义
    e_over_limit EXCEPTION;
END PKG_ORDER_MANAGE;
/

-- 创建包体
CREATE OR REPLACE PACKAGE BODY "PKG_ORDER_MANAGE" AS
    -- 私有变量
    v_process_count INT := 0;
    
    -- 实现过程
    PROCEDURE add_order(p_customer_id INT, p_amount NUMBER) AS
    BEGIN
        IF p_amount > g_max_amount THEN
            RAISE e_over_limit;
        END IF;
        
        INSERT INTO orders (customer_id, amount) VALUES (p_customer_id, p_amount);
        v_process_count := v_process_count + 1;
    END;
    
    -- 实现函数
    FUNCTION get_order_count(p_customer_id INT) RETURN INT AS
        v_count INT;
    BEGIN
        SELECT COUNT(*) INTO v_count FROM orders WHERE customer_id = p_customer_id;
        RETURN v_count;
    END;
    
    -- 初始化部分
BEGIN
    DBMS_OUTPUT.PUT_LINE('包初始化，最大金额: ' || g_max_amount);
END PKG_ORDER_MANAGE;
/

-- 调用包内元素
BEGIN
    PKG_ORDER_MANAGE.add_order(1001, 5000);
    DBMS_OUTPUT.PUT_LINE('订单数: ' || PKG_ORDER_MANAGE.get_order_count(1001));
END;
/
```

---

### 5.5 动态 SQL（DM 特有）

```sql
CREATE OR REPLACE PROCEDURE "EXEC_DYNAMIC_SQL"(p_table_name IN VARCHAR2)
AS
    v_sql VARCHAR2(2000);
    v_count INT;
BEGIN
    -- 构建动态 SQL
    v_sql := 'SELECT COUNT(*) FROM ' || p_table_name || ' WHERE status = ''active''';
    
    -- 执行并获取结果
    EXECUTE IMMEDIATE v_sql INTO v_count;
    
    DBMS_OUTPUT.PUT_LINE(p_table_name || ' 中活跃记录数: ' || v_count);
    
    -- 动态 DDL
    v_sql := 'CREATE TABLE temp_' || p_table_name || ' AS SELECT * FROM ' || p_table_name;
    EXECUTE IMMEDIATE v_sql;
END;
/
```

---

### 5.6 游标操作

```sql
CREATE OR REPLACE PROCEDURE "PROC_BATCH_UPDATE"
AS
    -- 显式游标
    CURSOR c_orders IS
        SELECT order_id, order_amount FROM orders WHERE status = 'pending';
    
    v_order_id orders.order_id%TYPE;
    v_amount orders.order_amount%TYPE;
BEGIN
    OPEN c_orders;
    LOOP
        FETCH c_orders INTO v_order_id, v_amount;
        EXIT WHEN c_orders%NOTFOUND;
        
        -- 处理逻辑
        UPDATE orders SET status = 'processing' WHERE order_id = v_order_id;
        
        -- 每100条提交一次
        IF MOD(c_orders%ROWCOUNT, 100) = 0 THEN
            COMMIT;
        END IF;
    END LOOP;
    CLOSE c_orders;
    COMMIT;
END;
/

-- 使用 REF CURSOR（返回结果集）
CREATE OR REPLACE FUNCTION "FUNC_GET_ORDERS"(p_cust_id INT)
RETURN SYS_REFCURSOR
AS
    c_result SYS_REFCURSOR;
BEGIN
    OPEN c_result FOR
        SELECT order_id, order_amount, create_time
        FROM orders
        WHERE customer_id = p_cust_id
        ORDER BY create_time DESC;
    
    RETURN c_result;
END;
/
```

---

## 六、函数与表达式

### 6.1 字符串函数

| 功能 | DM 语法 | MySQL 差异 |
|------|---------|------------|
| 拼接 | `str1 \|\| str2` 或 `CONCAT(str1, str2)` | MySQL 仅支持 `CONCAT`  |
| 子串 | `SUBSTR(str, pos, len)` | MySQL 支持 `SUBSTRING` 和 `SUBSTR` |
| 查找 | `INSTR(str, sub, pos, n)` | MySQL 为 `LOCATE(sub, str, pos)` |
| 长度 | `LENGTH(str)` / `LENGTHB(str)` | `LENGTHB` 返回字节数 |
| 替换 | `REPLACE(str, old, new)` | 语法相同 |
| 去空格 | `LTRIM(str)` / `RTRIM(str)` / `TRIM(str)` | 相同 |
| 大小写 | `UPPER(str)` / `LOWER(str)` | 相同 |

**DM 特有**：`NLSSORT(str, 'NLS_SORT = SCHINESE_PINYIN_M')` -- 拼音排序

---

### 6.2 日期函数

```sql
-- 当前时间
SELECT SYSDATE FROM dual;           -- 年月日 时分秒
SELECT SYSTIMESTAMP FROM dual;      -- 带时区的精确时间戳

-- 日期加减
SELECT DATEADD(DAY, 7, SYSDATE) AS next_week;      -- 加7天
SELECT DATEADD(MONTH, -1, SYSDATE) AS last_month;  -- 减1月

-- 日期差值
SELECT DATEDIFF(DAY, '2024-01-01', SYSDATE) AS days_diff;

-- 日期截断
SELECT DATE_TRUNC('MONTH', SYSDATE) AS month_start;  -- 月初
SELECT DATE_TRUNC('YEAR', SYSDATE) AS year_start;    -- 年初

-- 日期格式化
SELECT TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') AS format_date;  -- 日期转字符串
SELECT TO_DATE('2024-01-01', 'YYYY-MM-DD') AS date_value;          -- 字符串转日期

-- 提取日期部分
SELECT EXTRACT(YEAR FROM SYSDATE) AS year_num;
SELECT EXTRACT(MONTH FROM SYSDATE) AS month_num;
SELECT EXTRACT(DAY FROM SYSDATE) AS day_num;
```

**与 MySQL 差异**：DM 不支持 `DATE_SUB`，使用 `DATEADD` 负数替代；不支持 `DATE_FORMAT`，使用 `TO_CHAR` 替代 

---

### 6.3 数值函数

```sql
-- 四舍五入
SELECT ROUND(123.456, 2);  -- 123.46

-- 取整
SELECT CEIL(123.1);    -- 124
SELECT FLOOR(123.9);   -- 123

-- 取模
SELECT MOD(10, 3);     -- 1

-- 随机数
SELECT DBMS_RANDOM.VALUE(1, 100) AS random_num;  -- 生成 1-100 随机数

-- 空值处理
SELECT NVL(NULL, 0) AS value;      -- 替代 NULL
SELECT NVL2(expr1, val1, val2);    -- 条件空值处理
SELECT COALESCE(expr1, expr2, 0);  -- 返回第一个非空值
```

---

### 6.4 聚合与窗口函数

```sql
-- 标准聚合
SELECT 
    COUNT(*) AS total_count,
    SUM(order_amount) AS total_amount,
    AVG(order_amount) AS avg_amount,
    MAX(order_amount) AS max_amount,
    MIN(order_amount) AS min_amount,
    LISTAGG(customer_name, ',') AS all_customers  -- 替代 MySQL GROUP_CONCAT 
FROM orders;

-- 窗口函数
SELECT 
    order_id,
    customer_name,
    order_amount,
    ROW_NUMBER() OVER (ORDER BY order_amount DESC) AS rank1,
    RANK() OVER (PARTITION BY customer_name ORDER BY order_amount) AS rank2,
    DENSE_RANK() OVER (PARTITION BY region ORDER BY order_amount) AS rank3,
    LAG(order_amount, 1) OVER (ORDER BY create_time) AS prev_amount,
    LEAD(order_amount, 1) OVER (ORDER BY create_time) AS next_amount,
    SUM(order_amount) OVER (PARTITION BY customer_name ORDER BY create_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM orders;
```

---

### 6.5 条件函数

```sql
-- DM 特有：DECODE 函数（Oracle 风格）
SELECT DECODE(order_status, 
             'pending', '待支付',
             'paid', '已支付',
             'cancelled', '已取消',
             '未知状态') AS status_desc
FROM orders;

-- 标准 CASE
SELECT CASE 
         WHEN order_amount >= 10000 THEN '大额订单'
         WHEN order_amount >= 5000 THEN '中等订单'
         ELSE '小额订单'
       END AS order_level
FROM orders;
```

**与 MySQL 差异**：DM **不支持** `IF(condition, true_val, false_val)` 函数，需用 `CASE` 或 `DECODE` 替代 

---

### 6.6 类型转换函数

```sql
-- 显式转换
SELECT CAST('123' AS INT) AS int_val;
SELECT CAST(SYSDATE AS VARCHAR2(20)) AS date_str;
SELECT CONVERT('GBK', 'UTF8', '中文测试') AS converted;  -- 字符集转换

-- TO_ 系列函数
SELECT TO_CHAR(12345.67, 'L99,999.99') AS money_format;  -- 本地货币格式
SELECT TO_NUMBER('123.45', '999.99') AS num_val;
SELECT TO_DATE('2024年1月1日', 'YYYY"年"MM"月"DD"日"') AS date_val;

-- DM 特有：十六进制处理
SELECT HEX('ABC') AS hex_value;      -- 字符串转16进制
SELECT UNHEX('414243') AS str_value; -- 16进制转字符串
```

---

## 七、达梦数据库系统视图与元数据查询

```sql
-- 查询所有表
SELECT * FROM ALL_TABLES WHERE OWNER = 'SYSDBA';
SELECT * FROM USER_TABLES;  -- 当前用户下的表

-- 查询表结构
SELECT * FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = 'ORDERS';

-- 查询索引
SELECT * FROM ALL_INDEXES WHERE TABLE_OWNER = 'SYSDBA';

-- 查询约束
SELECT * FROM ALL_CONSTRAINTS WHERE OWNER = 'SYSDBA';

-- 查询视图定义
SELECT * FROM ALL_VIEWS WHERE OWNER = 'SYSDBA';

-- 查询存储过程
SELECT * FROM ALL_PROCEDURES WHERE OWNER = 'SYSDBA';
SELECT TEXT FROM ALL_SOURCE WHERE NAME = 'PROC_PROCESS_ORDER' ORDER BY LINE;

-- 查询序列
SELECT * FROM ALL_SEQUENCES WHERE SEQUENCE_OWNER = 'SYSDBA';

-- 查询会话信息
SELECT * FROM V$SESSIONS;  -- 当前所有会话
SELECT * FROM V$SQL_HISTORY WHERE SESSID = SESS_ID();  -- 当前会话的 SQL 历史

-- 查询锁信息
SELECT * FROM V$LOCK;
SELECT * FROM V$DEADLOCK_HISTORY;

-- 查询性能视图
SELECT * FROM V$SQL_STATISTICS WHERE SQL_TEXT LIKE '%orders%';
```

---

## 八、达梦特有性能优化语法

### 8.1 查询提示（Hints）

```sql
-- 全表扫描提示
SELECT /*+ FULL(orders) */ * FROM orders WHERE order_status = 'paid';

-- 索引提示
SELECT /*+ INDEX(orders idx_customer_name) */ * 
FROM orders WHERE customer_name = '张三';

-- 并行查询提示
SELECT /*+ PARALLEL(orders 4) */ COUNT(*) FROM orders;

-- 结果集缓存提示
SELECT /*+ RESULT_CACHE */ * FROM products WHERE product_id = 1001;

-- 不使用缓存
SELECT /*+ NO_RESULT_CACHE */ * FROM orders WHERE create_time > SYSDATE - 7;

-- 连接顺序提示
SELECT /*+ ORDERED */ *
FROM orders o, customer c, order_items i
WHERE o.customer_id = c.id AND o.order_id = i.order_id;
```

---

### 8.2 执行计划分析

```sql
-- 查看执行计划
EXPLAIN SELECT * FROM orders WHERE customer_name = '张三';

-- 详细执行计划
EXPLAIN SELECT * FROM orders o 
JOIN customer c ON o.customer_id = c.id 
WHERE o.order_amount > 10000;

-- 实际执行统计
EXPLAIN SELECT /*+ GATHER_PLAN_STATISTICS */ * 
FROM orders WHERE order_date BETWEEN '2024-01-01' AND '2024-12-31';
```

---

## 九、XML 与 JSON 支持（DM 特有）

```sql
-- XML 解析
SELECT EXTRACTVALUE(xml_column, '/root/item/name') AS item_name
FROM xml_table;

-- XML 生成
SELECT XMLELEMENT("Order", 
           XMLATTRIBUTES(order_id AS "id"),
           XMLELEMENT("Customer", customer_name)
       ) AS order_xml
FROM orders;

-- JSON 支持（DM 7.6+）
SELECT JSON_OBJECT('orderId' VALUE order_id, 'amount' VALUE order_amount) AS json_str
FROM orders;

-- JSON 查询
SELECT JSON_VALUE(json_column, '$.customer.name') AS cust_name
FROM json_table;

-- JSON 数组
SELECT JSON_ARRAYAGG(customer_name) AS name_list
FROM customer;
```

---

## 十、DM SQL 使用规范与最佳实践

1. **大小写规范**：对象名统一使用大写，避免双引号带来的大小写敏感问题
2. **模式前缀**：始终显式指定模式名（如 `"SYSDBA".table`），避免歧义
3. **注释规范**：使用 `COMMENT ON` 添加注释，而非列定义后注释
4. **类型选择**：优先使用 `VARCHAR2`、`NUMBER`、`CLOB` 等 DM 原生类型
5. **自增列**：推荐使用 `IDENTITY`，复杂场景使用 `SEQUENCE`
6. **分页查询**：优先使用标准 `OFFSET FETCH` 语法
7. **空字符串**：应用层拦截空字符串，统一转为 `NULL` 或特殊标记
8. **日期函数**：封装统一日期工具函数，屏蔽 `SYSDATE` 与 `NOW()` 差异
9. **错误处理**：存储过程中务必包含 `EXCEPTION` 块
10. **性能监控**：定期查询 `V$SQL_STATISTICS` 优化慢查询

---

## 十一、完整迁移示例：MySQL 到 DM

### MySQL 原表
```sql
CREATE TABLE `customer` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '客户ID',
  `name` varchar(200) NOT NULL COMMENT '姓名',
  `level` enum('VIP','Normal','New') DEFAULT 'New' COMMENT '等级',
  `profile` longtext COMMENT '档案',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### DM 转换后
```sql
-- 1. 创建表（无 COMMENT）
CREATE TABLE "CRM"."customer" (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,  -- 自增列
    name VARCHAR2(200) NOT NULL,
    level VARCHAR2(20) DEFAULT 'New',  -- ENUM 改 VARCHAR2
    profile CLOB,  -- LONGTEXT 改 CLOB
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. 添加注释
COMMENT ON TABLE "CRM".customer IS '客户表';
COMMENT ON COLUMN "CRM".customer.id IS '客户ID';
COMMENT ON COLUMN "CRM".customer.name IS '姓名';
COMMENT ON COLUMN "CRM".customer.level IS '等级';
COMMENT ON COLUMN "CRM".customer.profile IS '档案';
COMMENT ON COLUMN "CRM".customer.created_at IS '创建时间';

-- 3. 添加约束替代 ENUM
ALTER TABLE "CRM".customer 
ADD CONSTRAINT "chk_customer_level" 
CHECK (level IN ('VIP', 'Normal', 'New'));

-- 4. 创建索引
CREATE INDEX "idx_customer_name" ON "CRM".customer(name);
```

---

## 十二、总结：DM SQL 核心特点

| 特性类别 | 达梦(DM) 特点 | 与 MySQL 主要差异 |
|----------|---------------|-------------------|
| **架构** | 模式（SCHEMA）强制，用户与模式分离 | MySQL 的 DATABASE ≈ DM 的 SCHEMA |
| **数据类型** | VARCHAR2、NUMBER、CLOB/BLOB、IDENTITY | 无 TEXT/BLOB 系列，无 ENUM/SET |
| **注释** | 必须用 `COMMENT ON` | 不支持列后 COMMENT  |
| **自增** | IDENTITY 或 SEQUENCE | AUTO_INCREMENT  |
| **字符串** | `''` 视为 NULL，`||` 拼接 | 支持空字符串，`CONCAT` 为主 |
| **日期** | DATEADD/TO_CHAR，精度到微秒 | DATE_SUB/DATE_FORMAT  |
| **函数** | 支持 PL/SQL、DECODE、LISTAGG | 不支持 IF 函数  |
| **分页** | OFFSET FETCH / ROWNUM | LIMIT OFFSET |
| **高级** | 物化视图、同义词、位图索引、CONNECT BY | MySQL 8.0+ 部分支持 |
| **兼容性** | 兼容 Oracle 90% 以上 | 与 MySQL 差异较大 |

**最终建议**：迁移时，**业务系统必须修改**，建议采用分层设计，将数据库差异封装在 DAO 层，优先使用达梦原生语法以发挥最佳性能。