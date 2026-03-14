package types

import "time"

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
