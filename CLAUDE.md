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
  host: "192.168.189.200"
  port: 2003
  username: "test"
  password: "Gepoint"
  database: "osrdb"

migration:
  tables: []                # 空=迁移所有表，或指定表名列表
  migrate_views: true
  migrate_indexes: true
  parallelism: 4
  batch_size: 1000
  overwrite: false
```


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

## 迁移规则
### 创建表
创建表的时候不添加主键或者其他约束，在第二阶段完成之后再使用SQL语句新增主键ALTER TABLE 表名 ADD PRIMARY KEY(列名);

### 字段名
目标数据库的列字段名全部转成小写

### comment注释
在创建表的时候无需添加comment，在表创建成功之后再通过SQL语句添加comment

- 添加表注释示例:COMMENT ON TABLE users IS '用户信息表';
- 添加列注释示例:COMMENT ON COLUMN users.id IS '用户唯一标识符';

### 行数据
如果在第一阶段表结构创建失败的表，那么后续就不要再迁移数据以及索引等数据库对象

### 自增列
神通数据库的自增列需要先创建唯一索引再修改为自增列
比如:
create unique index 唯一索引名称 on 表名(自增列);
ALTER TABLE 表名 ALTER TYPE 自增列 INT AUTO_INCREMENT;

## 日志
每次迁移创建一个log目录里面再按照时间戳比如2026_03_10_15_19_01，如果在迁移的时候表、索引、约束、外键、视图、序列、自增列、表数据分别
生成tableCreateFailed.log,FkCreateFailed.log,idxCreateFailed.log,seqCreateFailed.log,viewCreateFailed.log,constraintFailed.log, errorTableData.log