# mysql2oscar

MySQL 到神通数据库(Oscar)的数据迁移工具

## 项目简介

mysql2oscar 是一个用 Go 语言编写的数据库迁移工具，专门用于将 MySQL 数据库迁移到神通数据库(Oscar)。该工具支持完整的数据库对象迁移，包括表结构、数据、视图、索引、约束和自增列，并针对大数据量场景进行了性能优化。

## 功能特性

- **完整的数据库对象迁移**
  - 表结构（包括列、主键、外键）
  - 表数据（支持并行和分片）
  - 视图（自动转换 SQL 语法）
  - 索引和约束
  - 自增列（自动创建序列）

- **高性能迁移**
  - Goroutine pool 并行处理
  - 批量插入（默认 1000 行/批次）
  - 流式数据读取，避免内存溢出
  - 分片迁移支持
    - 整数主键：范围分片（Range Chunking）
    - 字符串主键：偏移分片（Offset Chunking）

- **灵活的迁移配置**
  - 支持整个数据库迁移
  - 支持指定表列表迁移
  - 可配置并行度和批处理大小
  - 可选择是否迁移视图、索引

- **完善的错误处理**
  - 详细的日志记录
  - 失败对象分类记录
  - 连接断开自动重连
  - 分片级别重试机制

## 环境要求

- Go 1.16 或更高版本
- MySQL 5.7+ / 8.0+
- 神通数据库(Oscar)
- go-aci 驱动（用于连接 Oscar）

## 安装与构建

### 1. 克隆项目

```bash
git clone https://github.com/iverycd/mysql2oscar.git
cd mysql2oscar
```

### 2. 设置代理（可选）

如果网络不好，可以设置 Go 代理：

```bash
export GOPROXY=https://goproxy.cn,direct
```

### 3. 整理依赖

```bash
go mod tidy
```

### 4. 神通go-aci库部署

```bash
将此项目的go-aci目录复制到go环境
比如：
cp -apr go-aci /usr/local/go/src/go-aci

设定ACI库的环境变量
export PKG_CONFIG_PATH=/usr/local/go/src/go-aci:$PKG_CONFIG_PATH
export LD_LIBRARY_PATH=/usr/local/go/src/go-aci/lib/linux64:$LD_LIBRARY_PATH

运行测试代码示例
go run example/conn_oscar.go 账号/密码@192.168.1.2:2003/OSRDB
```

### 5. 开发环境运行
```bash
go run mysql2oscar/cmd/mysql2oscar -config config.yaml
```

### 6. 编译项目

```bash
go build -o mysql2oscar ./cmd/mysql2oscar
```

### 7. 运行程序

```bash
./mysql2oscar -config config.yaml
```

## 配置说明

### 配置文件格式

创建 `config.yaml` 文件：

```yaml
# MySQL 源数据库配置
source:
  host: "192.168.1.200"
  port: 3306
  user: "root"
  password: "password"
  database: "mydb"
  charset: "utf8mb4"

# Oscar 目标数据库配置
target:
  host: "192.168.2.2"
  port: 2003
  username: "test"
  password: "password"
  database: "osrdb"

# 迁移配置
migration:
  # 要迁移的表列表，为空表示迁移整个数据库
  tables: []
  # 是否迁移视图
  migrate_views: true
  # 是否迁移索引
  migrate_indexes: true
  # 并行处理数（表级别并行度）
  parallelism: 4
  # 数据批处理大小
  batch_size: 200
  # 是否覆盖已存在的表
  overwrite: true
  # 是否启用分片迁移
  enable_chunking: true
  # 每个分片的行数
  chunk_size: 10000
  # 单表内分片并行度
  chunk_parallelism: 2
  # 启用分片的阈值行数，小于此值的表不分片
  chunk_threshold: 50000
```

### 配置项详解

#### source（MySQL 源数据库配置）

| 配置项 | 类型 | 必填 | 默认值 | 说明 |
|--------|------|------|--------|------|
| host | string | 是 | - | MySQL 主机地址 |
| port | int | 否 | 3306 | MySQL 端口 |
| user | string | 是 | - | 用户名 |
| password | string | 是 | - | 密码 |
| database | string | 是 | - | 数据库名 |
| charset | string | 否 | utf8mb4 | 字符集 |

#### target（Oscar 目标数据库配置）

| 配置项 | 类型 | 必填 | 默认值 | 说明 |
|--------|------|------|--------|------|
| host | string | 是 | - | Oscar 主机地址 |
| port | int | 是 | - | Oscar 端口 |
| username | string | 是 | - | 用户名 |
| password | string | 是 | - | 密码 |
| database | string | 是 | - | 数据库名 |

#### migration（迁移配置）

| 配置项 | 类型 | 必填 | 默认值 | 说明 |
|--------|------|------|--------|------|
| tables | []string | 否 | [] | 要迁移的表列表，空表示迁移所有表 |
| migrate_views | bool | 否 | true | 是否迁移视图 |
| migrate_indexes | bool | 否 | true | 是否迁移索引 |
| parallelism | int | 否 | 4 | 表级别并行度 |
| batch_size | int | 否 | 1000 | 批量插入行数 |
| overwrite | bool | 否 | false | 是否覆盖已存在的表 |
| enable_chunking | bool | 否 | false | 是否启用分片迁移 |
| chunk_size | int64 | 否 | 10000 | 每个分片的行数 |
| chunk_parallelism | int | 否 | 2 | 单表内分片并行度 |
| chunk_threshold | int64 | 否 | 50000 | 启用分片的阈值行数 |

## 使用方式

### 命令行参数

```bash
./mysql2oscar [选项]

选项:
  -config string
        配置文件路径 (默认 "config.yaml")
  -version
        显示版本信息
```

### 运行示例

1. **迁移整个数据库**

```bash
# 编辑 config.yaml，设置 tables 为空
./mysql2oscar -config config.yaml
```

2. **迁移指定表**

```yaml
# config.yaml
migration:
  tables:
    - users
    - orders
    - products
```

```bash
./mysql2oscar -config config.yaml
```

3. **查看版本信息**

```bash
./mysql2oscar -version
```

## 架构设计

### 目录结构

```
mysql2oscar/
├── cmd/
│   └── mysql2oscar/
│       └── main.go              # 程序入口
├── internal/
│   ├── config/
│   │   └── config.go            # 配置解析
│   ├── source/
│   │   └── mysql/
│   │       ├── client.go        # MySQL 连接管理
│   │       ├── schema.go        # 表结构读取
│   │       └── data.go          # 数据读取
│   ├── target/
│   │   └── oscar/
│   │       ├── client.go        # Oscar 连接管理
│   │       ├── schema.go        # 表结构创建
│   │       └── data.go          # 数据写入
│   ├── migrator/
│   │   ├── migrator.go          # 迁移编排器
│   │   └── logger.go            # 日志管理
│   └── transform/
│       ├── types.go             # MySQL -> Oscar 类型映射
│       └── view.go              # 视图 SQL 转换
├── pkg/
│   └── types/
│       └── schema.go            # 通用数据结构
├── go-aci/                      # Oscar ODBC 驱动
├── config.example.yaml          # 配置示例
└── README.md
```

### 核心模块

- **config**: 负责加载和解析 YAML 配置文件
- **source/mysql**: 负责从 MySQL 读取表结构、数据和视图定义
- **target/oscar**: 负责在 Oscar 中创建表结构、写入数据
- **migrator**: 迁移编排器，协调整个迁移流程
- **transform**: 负责类型映射和 SQL 语法转换

## 迁移流程

mysql2oscar 采用**四阶段迁移模式**，确保数据完整性和迁移成功率：

### 第一阶段：创建表结构

- **执行方式**：单线程串行
- **操作内容**：
  - 读取 MySQL 表结构
  - 转换数据类型
  - 创建 Oscar 表（不含主键、自增属性）
  - 添加表注释和列注释
- **失败处理**：记录失败表，后续阶段跳过

### 第二阶段：迁移数据

- **执行方式**：多线程并行
- **操作内容**：
  - 根据表大小选择迁移策略
  - 小表：单线程批量插入
  - 大表：分片并行迁移
    - 整数主键：范围分片（Range Chunking）
    - 字符串主键：偏移分片（Offset Chunking）
- **性能优化**：
  - Goroutine pool 并行处理
  - 批量插入（默认 1000 行/批次）
  - 流式读取，避免内存溢出
  - 连接断开自动重连
  - 分片级别重试（最多 3 次）

### 第三阶段：创建索引/约束/自增列

- **执行方式**：单线程串行
- **操作内容**：
  - 创建主键约束
  - 创建普通索引和唯一索引
  - 创建外键约束
  - 设置自增列（创建序列并设置默认值）
- **自增列处理**：
  1. 创建序列：`CREATE SEQUENCE table_col_seq START WITH x`
  2. 设置默认值：`ALTER TABLE table ALTER COLUMN col SET DEFAULT NEXTVAL('table_col_seq')`

### 第四阶段：迁移视图

- **执行方式**：单线程串行
- **操作内容**：
  - 读取 MySQL 视图定义
  - 转换 SQL 语法（移除数据库前缀、转换函数等）
  - 在 Oscar 中创建视图
- **SQL 转换**：
  - 移除数据库名前缀
  - 转换不兼容的函数
  - 处理引号差异

## 类型映射

### MySQL -> Oscar 类型映射表

| MySQL 类型 | Oscar 类型 | 说明 |
|-----------|-----------|------|
| TINYINT | SMALLINT | 小整数 |
| SMALLINT | SMALLINT | 短整数 |
| MEDIUMINT | INTEGER | 中整数 |
| INT / INTEGER | INTEGER | 整数 |
| BIGINT | BIGINT | 长整数 |
| FLOAT | FLOAT | 单精度浮点数 |
| DOUBLE | DOUBLE | 双精度浮点数 |
| DECIMAL | DECIMAL | 定点数 |
| NUMERIC | NUMERIC | 数值型 |
| CHAR | CHAR | 定长字符 |
| VARCHAR | VARCHAR | 变长字符 |
| TEXT / TINYTEXT / MEDIUMTEXT / LONGTEXT | CLOB | 大文本 |
| BLOB / TINYBLOB / MEDIUMBLOB / LONGBLOB | BLOB | 二进制大对象 |
| BINARY / VARBINARY | BLOB | 二进制数据 |
| DATE | DATE | 日期 |
| DATETIME | TIMESTAMP | 日期时间 |
| TIMESTAMP | TIMESTAMP | 时间戳 |
| TIME | TIME | 时间 |
| YEAR | SMALLINT | 年份 |
| ENUM / SET | VARCHAR(255) | 枚举/集合 |
| BIT | SMALLINT | 位字段 |
| BOOLEAN / BOOL | SMALLINT | 布尔值 |
| JSON | CLOB | JSON 数据 |

**注意事项**：
- UNSIGNED 和 SIGNED 修饰符会被移除（Oscar 不支持）
- 默认值中的位字面量（如 `b'1'`）会被转换为整数

## 迁移规则

### 神通数据库特殊处理

为了兼容神通数据库的特性，迁移时遵循以下规则：

#### 1. 分阶段创建约束

创建表时不添加主键和其他约束，在数据迁移完成后再添加：

```sql
-- 第一阶段：创建表（不含约束）
CREATE TABLE "users" (
  "id" INTEGER NOT NULL,
  "name" VARCHAR(255)
);

-- 第三阶段：添加主键
ALTER TABLE "users" ADD PRIMARY KEY ("id");
```

#### 2. 字段名转小写

目标数据库的列字段名全部转为小写，避免大小写敏感问题。

#### 3. 分阶段添加注释

创建表时不添加注释，表创建成功后再通过 SQL 语句添加：

```sql
-- 添加表注释
COMMENT ON TABLE users IS '用户信息表';

-- 添加列注释
COMMENT ON COLUMN users.id IS '用户唯一标识符';
COMMENT ON COLUMN users.name IS '用户名';
```

#### 4. 自增列特殊处理

神通数据库的自增列需要先创建序列，再设置为默认值：

```sql
-- 创建序列
CREATE SEQUENCE users_id_seq START WITH 1;

-- 设置列默认值为序列的下一个值
ALTER TABLE users ALTER COLUMN id SET DEFAULT NEXTVAL('users_id_seq');
```

#### 5. 失败表跳过机制

如果在第一阶段表结构创建失败，该表将被记录并跳过后续所有阶段的迁移（数据、索引、约束等）。

## 日志说明

### 日志结构

每次迁移会在 `log` 目录下创建一个以时间戳命名的子目录，例如：

```
log/
└── 2026_03_20_15_30_45/
    ├── tableCreateFailed.log    # 表结构创建失败的表
    ├── idxCreateFailed.log      # 索引创建失败的记录
    ├── FkCreateFailed.log       # 外键创建失败的记录
    ├── viewCreateFailed.log     # 视图创建失败的记录
    ├── constraintFailed.log     # 约束创建失败的记录
    ├── seqCreateFailed.log      # 序列创建失败的记录
    └── errorTableData.log       # 数据插入失败的表
```

### 日志格式

每个日志文件记录详细的错误信息，包括：

- 失败对象名称
- 失败的 SQL 语句
- 错误消息

示例：

```
[表: users]
SQL: CREATE TABLE "users" (...)
错误: 表已存在

[索引: idx_users_name]
SQL: CREATE INDEX "idx_users_name" ON "users" ("name")
错误: 列不存在
```

### 控制台输出

迁移过程中，控制台会实时输出进度信息：

```
[1/10] 创建表结构: users
[1/10] 表 users: 创建成功 (125ms)
========== 第二阶段: 迁移所有表数据 ==========
[数据 1/10] 表 users: 成功 - 50000 行 (3.2s)
[Worker-1] 表 orders 分片[0,10000): 已插入 5000 行
...
```

### 最终报告

迁移完成后，会输出汇总报告：

```
迁移完成!
  表迁移: 成功 10, 失败 2
  视图迁移: 成功 5, 失败 0
  索引/约束: 成功 15, 失败 1
  自增列: 成功 8, 失败 0
  总行数: 150000
  总耗时: 2m30s

失败的表:
  - orders_backup
  - temp_data
```

## 开发与测试

### 运行测试

```bash
# 运行所有测试
go test ./...

# 运行特定包的测试
go test ./internal/migrator
```

### 代码格式化

```bash
go fmt ./...
```

### 静态检查

```bash
go vet ./...
```

## 常见问题

### 1. 运行失败

**问题**: `error while loading shared libraries: libaci.so: cannot open shared object file: No such file or directory`

**解决方案**:
- 指定aci库的路径，比如export LD_LIBRARY_PATH=oscar_lib/linux64
```shell
[root@localhost ~]# ll -hrt oscar_lib/linux64
total 3.1M
-r--r--r--. 1 root root 1.6M Mar 20 09:58 libaci.so
-r--r--r--. 1 root root 1.3M Mar 20 09:58 libiconv.so.2
-r--r--r--. 1 root root 171K Mar 20 09:58 libsnappy.so.1
```

### 2. 表已存在

**问题**: `表已存在`

**解决方案**:
- 设置 `overwrite: true` 覆盖已存在的表
- 或手动删除目标表后重新运行

### 3. 内存溢出

**问题**: 迁移大表时内存不足

**解决方案**:
- 减小 `batch_size`（如设置为 500）
- 启用分片迁移：`enable_chunking: true`
- 减小 `chunk_size`（如设置为 5000）

### 4. 连接超时

**问题**: 迁移过程中连接断开

**解决方案**:
- 工具已内置自动重连机制
- 可以增加 `chunk_parallelism` 来减少单连接持续时间
- 检查网络稳定性

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！
