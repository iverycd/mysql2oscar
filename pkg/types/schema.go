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

	// 索引/约束/自增列统计
	IndexesSuccess     int      // 索引创建成功数
	IndexesFailed      int      // 索引创建失败数
	ConstraintsSuccess int      // 约束(主键+外键)创建成功数
	ConstraintsFailed  int      // 约束创建失败数
	AutoIncrSuccess    int      // 自增列设置成功数
	AutoIncrFailed     int      // 自增列设置失败数
	FailedDataTables   []string // 数据插入失败的表名列表
}

// ChunkMetadata 表示分片元数据
type ChunkMetadata struct {
	TableName  string
	ChunkID    int
	StartValue int64  // 起始主键值
	EndValue   int64  // 结束主键值（不包含）
	Status     string // pending/running/completed/failed
}

// ChunkStrategy 分片策略
type ChunkStrategy int

const (
	// ChunkStrategyNone 不分片（单线程）
	ChunkStrategyNone ChunkStrategy = iota
	// ChunkStrategyRange 基于整数主键范围分片
	ChunkStrategyRange
	// ChunkStrategyOffset 基于OFFSET/LIMIT分片（用于字符串/UUID主键）
	ChunkStrategyOffset
)

// ChunkPlan 分片计划
type ChunkPlan struct {
	Strategy     ChunkStrategy
	PKColumn     string        // 主键列名
	MinValue     int64         // 主键最小值（仅整数主键）
	MaxValue     int64         // 主键最大值（仅整数主键）
	ChunkSize    int64         // 每个分片的大小
	NumChunks    int           // 分片数量
	Chunks       []ChunkRange  // 分片范围列表（整数主键）
	OffsetChunks []OffsetChunk // 偏移分片列表（字符串主键）
}

// ChunkRange 单个分片的范围（整数主键）
type ChunkRange struct {
	Start int64
	End   int64
}

// OffsetChunk 偏移分片（字符串主键）
type OffsetChunk struct {
	ChunkID int
	Offset  int64
	Limit   int64
}
