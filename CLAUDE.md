# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**mysql2oscar** 是一个 Go 语言项目，用于 MySQL 到神通数据库(Oscar)的数据迁移工具。

### 功能特性
- 支持整个数据库迁移和指定表列表迁移
- 支持表结构、数据、视图、索引、约束、自增列迁移
- 并行处理大数据量（goroutine pool + 批量插入）
- 流式数据读取，避免内存溢出

## Build Commands

```bash
# 设置代理（如果网络不好）
export GOPROXY=https://goproxy.cn,direct

# 整理依赖
go mod tidy

# 构建项目
go build -o mysql2oscar ./cmd/mysql2oscar

# 运行程序
./mysql2oscar -config config.yaml

# 运行所有测试
go test ./...

# 代码格式化
go fmt ./...

# 静态检查
go vet ./...
```

## Architecture

```
mysql2oscar/
├── cmd/mysql2oscar/main.go       # 入口
├── internal/
│   ├── config/config.go          # 配置解析
│   ├── source/mysql/             # MySQL 源端
│   │   ├── client.go             # 连接管理
│   │   ├── schema.go             # 表结构读取
│   │   └── data.go               # 数据读取
│   ├── target/oscar/             # Oscar 目标端
│   │   ├── client.go             # ODBC 连接
│   │   ├── schema.go             # 表结构创建
│   │   └── data.go               # 数据写入
│   ├── migrator/migrator.go      # 迁移编排器
│   └── transform/                # 类型转换
│       ├── types.go              # MySQL -> Oscar 类型映射
│       └── view.go               # 视图 SQL 转换
├── pkg/types/schema.go           # 通用数据结构
└── config.example.yaml           # 配置示例
```

## Configuration

配置文件 `config.yaml`：

```yaml
source:
  host: "192.168.189.200"
  port: 3306
  user: "root"
  password: "password"
  database: "mydb"
  charset: "utf8mb4"

target:
  dsn: "OscarDSN"           # ODBC 数据源名称
  username: "oscar_user"
  password: "oscar_password"
  schema: "target_schema"

migration:
  tables: []                # 空=迁移所有表，或指定表名列表
  migrate_views: true
  migrate_indexes: true
  parallelism: 4
  batch_size: 1000
  overwrite: false
```

## ODBC 配置

### macOS 环境配置步骤

#### 1. 安装 unixODBC

```bash
brew install unixodbc
```

#### 2. 配置驱动 (`/usr/local/etc/odbcinst.ini`)

```ini
[Oscar]
Description = Oscar Database ODBC Driver
Driver      = /path/to/oscar/odbc/lib/liboscarodbcw.so
Setup       = /path/to/oscar/odbc/lib/liboscarodbcw.so
Threading   = 0
```

#### 3. 配置数据源 (`/usr/local/etc/odbc.ini`)

```ini
[OscarDSN]
Description = Oscar Database Connection
Driver      = Oscar
Server      = 192.168.219.92
Port        = 2003
Database    = osrdb
UserName    = test
Password    = Gepoint
```

#### 4. 测试连接

```bash
isql -v OscarDSN test Gepoint
```

### 重要说明

**Oscar ODBC 驱动兼容性问题：**
- 当前驱动位于 `/Users/kay/Documents/database/oscar/odbc/`
- 驱动文件是 **Linux ELF 格式**（`.so` 文件），无法直接在 macOS 上运行
- macOS 需要 Mach-O 格式的 `.dylib` 文件

### 运行方案

由于驱动格式限制，推荐以下方案：

#### 方案 A：Docker 容器运行（推荐）
在 Linux 容器中运行迁移工具，可以使用现有的 Linux 驱动。

#### 方案 B：获取 macOS 版驱动
联系神通官方获取 macOS 版本的 ODBC 驱动。

#### 方案 C：在 Linux 机器上运行
编译 Linux 版本：`GOOS=linux GOARCH=amd64 go build -o mysql2oscar-linux ./cmd/mysql2oscar`

## Type Mapping (MySQL -> Oscar)

| MySQL | Oscar |
|-------|-------|
| TINYINT | SMALLINT |
| SMALLINT | SMALLINT |
| MEDIUMINT | INTEGER |
| INT/INTEGER | INTEGER |
| BIGINT | BIGINT |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| DECIMAL | DECIMAL |
| CHAR/VARCHAR | CHAR/VARCHAR |
| TEXT系列 | CLOB |
| BLOB系列 | BLOB |
| DATE | DATE |
| DATETIME/TIMESTAMP | TIMESTAMP |
| ENUM/SET | VARCHAR(255) |