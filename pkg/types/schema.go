package types

import "time"

// PrimaryKeyType 主键类型枚举
type PrimaryKeyType int

const (
	// PKTypeNone 无主键
	PKTypeNone PrimaryKeyType = iota
	// PKTypeInteger 整数主键（支持范围分片）
	PKTypeInteger
	// PKTypeOther 其他类型主键（UUID/复合主键等）
	PKTypeOther
)

// ShardRange 数据分片范围
type ShardRange struct {
	// 范围分片（整数主键）
	StartValue interface{} // 分片起始值
	EndValue   interface{} // 分片结束值（不包含）
	// OFFSET 分片（其他类型主键）
	Offset int64 // 偏移量
	Limit  int64 // 分片大小
	// 通用
	ShardIndex int // 分片索引
}

// Column 表示数据库列的定义
type Column struct {
	Name         string
	DataType     string
	IsNullable   bool
	DefaultValue *string
	IsPrimary    bool
	IsAutoIncr   bool
	Comment      string
	// MySQL 特有属性
	CharMaxLength    int
	NumericPrecision int
	NumericScale     int
	EnumValues       []string
}

// Index 表示索引定义
type Index struct {
	Name      string
	Columns   []string
	IsUnique  bool
	IsPrimary bool
}

// ForeignKey 表示外键约束
type ForeignKey struct {
	Name              string
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
	OnDelete          string
	OnUpdate          string
}

// Table 表示表结构定义
type Table struct {
	Schema      string
	Name        string
	Columns     []Column
	Indexes     []Index
	ForeignKeys []ForeignKey
	Comment     string
}

// View 表示视图定义
type View struct {
	Schema     string
	Name       string
	Definition string
	Comment    string
}

// DataRow 表示一行数据
type DataRow struct {
	Values []interface{}
}

// DataBatch 表示一批数据
type DataBatch struct {
	Columns []string
	Rows    []DataRow
}

// MigrationProgress 表示迁移进度
type MigrationProgress struct {
	TableName    string
	TotalRows    int64
	MigratedRows int64
	StartTime    time.Time
	EndTime      *time.Time
	Error        error
}

// MigrationResult 表示迁移结果
type MigrationResult struct {
	TablesMigrated int
	TablesFailed   int
	ViewsMigrated  int
	ViewsFailed    int
	TotalRows      int64
	TotalTime      time.Duration
	FailedTables   []string
	FailedViews    []string
}
