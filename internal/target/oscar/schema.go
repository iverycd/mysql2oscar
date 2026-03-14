package oscar

import (
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"

	"mysql2oscar/pkg/types"
)

// SchemaWriter 表结构写入器
type SchemaWriter struct {
	client *Client
}

// NewSchemaWriter 创建表结构写入器
func NewSchemaWriter(client *Client) *SchemaWriter {
	return &SchemaWriter{client: client}
}

// CreateTableWithoutAutoIncr 创建表但不包含自增属性（用于神通数据库）
// 神通数据库需要先创建表，再创建唯一索引，最后设置自增属性
// 返回生成的 SQL 语句和可能的错误
func (w *SchemaWriter) CreateTableWithoutAutoIncr(table *types.Table) (string, error) {
	// 检查列是否为空
	if len(table.Columns) == 0 {
		return "", fmt.Errorf("创建表 %s 失败: 表没有定义任何列", table.Name)
	}

	var sql strings.Builder

	// 构建建表语句
	sql.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", w.quoteIdentifier(table.Name)))

	// 列定义（不包含 AUTO_INCREMENT）
	for i, col := range table.Columns {
		sql.WriteString(fmt.Sprintf("  %s %s", w.quoteIdentifier(col.Name), w.convertType(col.DataType)))

		if !col.IsNullable {
			sql.WriteString(" NOT NULL")
		}

		// 注意：这里不添加 AUTO_INCREMENT，稍后单独处理

		if col.DefaultValue != nil && !col.IsAutoIncr {
			sql.WriteString(fmt.Sprintf(" DEFAULT %s", w.formatDefault(*col.DefaultValue)))
		}

		// 注意：神通数据库不支持在建表时添加 COMMENT，需要在表创建后单独添加

		if i < len(table.Columns)-1 {
			sql.WriteString(",\n")
		}
	}

	sql.WriteString("\n)")

	// 注意：神通数据库不支持在建表时添加表注释，需要在表创建后单独添加

	sqlStr := sql.String()

	// 执行建表语句
	_, err := w.client.Exec(sqlStr)
	if err != nil {
		return sqlStr, fmt.Errorf("创建表 %s 失败: %w", table.Name, err)
	}

	return sqlStr, nil
}

// AddPrimaryKey 添加主键约束
// 在表创建完成后，使用 ALTER TABLE 添加主键
// 返回生成的 SQL 语句和可能的错误
func (w *SchemaWriter) AddPrimaryKey(tableName string, columns []string) (string, error) {
	if len(columns) == 0 {
		return "", nil
	}

	cols := make([]string, len(columns))
	for i, c := range columns {
		cols[i] = w.quoteIdentifier(c)
	}

	sql := fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY (%s)",
		w.quoteIdentifier(tableName),
		strings.Join(cols, ", "))

	if _, err := w.client.Exec(sql); err != nil {
		return sql, fmt.Errorf("添加主键失败: %w", err)
	}

	return sql, nil
}

// DropSequence 删除序列
// 返回生成的 SQL 语句和可能的错误
func (w *SchemaWriter) DropSequence(tableName, columnName string) (string, error) {
	// 序列名称格式：seq_表名_列名（全小写）
	seqName := fmt.Sprintf("seq_%s_%s", strings.ToLower(tableName), strings.ToLower(columnName))
	sql := fmt.Sprintf("DROP SEQUENCE IF EXISTS %s", w.quoteIdentifier(seqName))

	if _, err := w.client.Exec(sql); err != nil {
		return sql, fmt.Errorf("删除序列失败: %w", err)
	}

	return sql, nil
}

// CreateSequence 创建序列
// startValue: 序列起始值（从MySQL的Auto_increment获取）
// 返回生成的 SQL 语句和可能的错误
func (w *SchemaWriter) CreateSequence(tableName, columnName string, startValue int64) (string, error) {
	// 序列名称格式：seq_表名_列名（全小写）
	seqName := fmt.Sprintf("seq_%s_%s", strings.ToLower(tableName), strings.ToLower(columnName))
	sql := fmt.Sprintf("CREATE SEQUENCE %s INCREMENT BY 1 START %d",
		w.quoteIdentifier(seqName), startValue)

	if _, err := w.client.Exec(sql); err != nil {
		return sql, fmt.Errorf("创建序列失败: %w", err)
	}

	return sql, nil
}

// SetColumnDefaultSequence 设置列的默认值为序列的下一个值
// 返回生成的 SQL 语句和可能的错误
func (w *SchemaWriter) SetColumnDefaultSequence(tableName, columnName string) (string, error) {
	// 序列名称格式：seq_表名_列名（全小写）
	seqName := fmt.Sprintf("seq_%s_%s", strings.ToLower(tableName), strings.ToLower(columnName))
	// 神通数据库语法：ALTER TABLE 表名 ALTER COLUMN 列名 SET DEFAULT nextval('序列名')
	sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT nextval('%s')",
		w.quoteIdentifier(tableName),
		w.quoteIdentifier(columnName),
		strings.ToLower(seqName))

	if _, err := w.client.Exec(sql); err != nil {
		return sql, fmt.Errorf("设置列默认值为序列失败: %w", err)
	}

	return sql, nil
}

// CreateIndexes 创建索引
// 返回失败的索引信息列表（索引名, SQL, 错误）
func (w *SchemaWriter) CreateIndexes(tableName string, indexes []types.Index) []IndexError {
	var failedIndexes []IndexError

	for _, idx := range indexes {
		if idx.IsPrimary {
			continue // 主键在建表时已创建
		}

		sql, err := w.CreateSingleIndex(tableName, idx)
		if err != nil {
			failedIndexes = append(failedIndexes, IndexError{
				IndexName: idx.Name,
				SQL:       sql,
				Err:       err,
			})
		}
	}

	return failedIndexes
}

// IndexError 索引创建错误信息
type IndexError struct {
	IndexName string
	SQL       string
	Err       error
}

// CreateSingleIndex 创建单个索引
// 返回生成的 SQL 语句和可能的错误
func (w *SchemaWriter) CreateSingleIndex(tableName string, idx types.Index) (string, error) {
	cols := make([]string, len(idx.Columns))
	for i, c := range idx.Columns {
		cols[i] = w.quoteIdentifier(c)
	}

	indexType := "INDEX"
	if idx.IsUnique {
		indexType = "UNIQUE INDEX"
	}

	// 生成新的索引名称：idx_表名前8字符_列名前5字符_3位随机数字
	newIndexName := w.generateIndexName(tableName, idx.Columns[0])

	sql := fmt.Sprintf("CREATE %s %s ON %s (%s)",
		indexType,
		w.quoteIdentifier(newIndexName),
		w.quoteIdentifier(tableName),
		strings.Join(cols, ", "))

	if _, err := w.client.Exec(sql); err != nil {
		return sql, fmt.Errorf("创建索引 %s 失败: %w", idx.Name, err)
	}

	return sql, nil
}

// CreateForeignKeys 创建外键
func (w *SchemaWriter) CreateForeignKeys(tableName string, fks []types.ForeignKey) error {
	for _, fk := range fks {
		cols := make([]string, len(fk.Columns))
		for i, c := range fk.Columns {
			cols[i] = w.quoteIdentifier(c)
		}

		refCols := make([]string, len(fk.ReferencedColumns))
		for i, c := range fk.ReferencedColumns {
			refCols[i] = w.quoteIdentifier(c)
		}

		sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
			w.quoteIdentifier(tableName),
			w.quoteIdentifier(fk.Name),
			strings.Join(cols, ", "),
			w.quoteIdentifier(fk.ReferencedTable),
			strings.Join(refCols, ", "))

		if fk.OnDelete != "" && fk.OnDelete != "NO ACTION" {
			sql += fmt.Sprintf(" ON DELETE %s", fk.OnDelete)
		}
		if fk.OnUpdate != "" && fk.OnUpdate != "NO ACTION" {
			sql += fmt.Sprintf(" ON UPDATE %s", fk.OnUpdate)
		}

		if _, err := w.client.Exec(sql); err != nil {
			return fmt.Errorf("创建外键 %s 失败: %w", fk.Name, err)
		}
	}

	return nil
}

// CreateSingleForeignKey 创建单个外键
// 返回生成的 SQL 语句和可能的错误
func (w *SchemaWriter) CreateSingleForeignKey(tableName string, fk types.ForeignKey) (string, error) {
	cols := make([]string, len(fk.Columns))
	for i, c := range fk.Columns {
		cols[i] = w.quoteIdentifier(c)
	}

	refCols := make([]string, len(fk.ReferencedColumns))
	for i, c := range fk.ReferencedColumns {
		refCols[i] = w.quoteIdentifier(c)
	}

	sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
		w.quoteIdentifier(tableName),
		w.quoteIdentifier(fk.Name),
		strings.Join(cols, ", "),
		w.quoteIdentifier(fk.ReferencedTable),
		strings.Join(refCols, ", "))

	if fk.OnDelete != "" && fk.OnDelete != "NO ACTION" {
		sql += fmt.Sprintf(" ON DELETE %s", fk.OnDelete)
	}
	if fk.OnUpdate != "" && fk.OnUpdate != "NO ACTION" {
		sql += fmt.Sprintf(" ON UPDATE %s", fk.OnUpdate)
	}

	if _, err := w.client.Exec(sql); err != nil {
		return sql, fmt.Errorf("创建外键 %s 失败: %w", fk.Name, err)
	}

	return sql, nil
}

// AddTableComment 添加表注释
// 返回生成的 SQL 语句和可能的错误
func (w *SchemaWriter) AddTableComment(tableName, comment string) (string, error) {
	if comment == "" {
		return "", nil
	}

	sql := fmt.Sprintf("COMMENT ON TABLE %s IS '%s'",
		w.quoteIdentifier(tableName),
		strings.ReplaceAll(comment, "'", "''"))

	if _, err := w.client.Exec(sql); err != nil {
		return sql, fmt.Errorf("添加表注释失败: %w", err)
	}

	return sql, nil
}

// AddColumnComment 添加列注释
// 返回生成的 SQL 语句和可能的错误
func (w *SchemaWriter) AddColumnComment(tableName, columnName, comment string) (string, error) {
	if comment == "" {
		return "", nil
	}

	sql := fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s'",
		w.quoteIdentifier(tableName),
		w.quoteIdentifier(columnName),
		strings.ReplaceAll(comment, "'", "''"))

	if _, err := w.client.Exec(sql); err != nil {
		return sql, fmt.Errorf("添加列注释失败: %w", err)
	}

	return sql, nil
}

// AddColumnComments 批量添加列注释
// 返回失败的列注释信息列表（列名, SQL, 错误）
func (w *SchemaWriter) AddColumnComments(tableName string, columns []types.Column) []ColumnCommentError {
	var failedComments []ColumnCommentError

	for _, col := range columns {
		if col.Comment == "" {
			continue
		}

		sql, err := w.AddColumnComment(tableName, col.Name, col.Comment)
		if err != nil {
			failedComments = append(failedComments, ColumnCommentError{
				ColumnName: col.Name,
				SQL:        sql,
				Err:        err,
			})
		}
	}

	return failedComments
}

// ColumnCommentError 列注释错误信息
type ColumnCommentError struct {
	ColumnName string
	SQL        string
	Err        error
}

// CreateView 创建视图
// 返回生成的 SQL 语句和可能的错误
func (w *SchemaWriter) CreateView(view *types.View) (string, error) {
	sql := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s",
		w.quoteIdentifier(view.Name),
		view.Definition)

	if _, err := w.client.Exec(sql); err != nil {
		return sql, fmt.Errorf("创建视图 %s 失败: %w", view.Name, err)
	}

	return sql, nil
}

// convertType 将 MySQL 类型转换为 Oscar 类型
func (w *SchemaWriter) convertType(mysqlType string) string {
	upperType := strings.ToUpper(mysqlType)

	// 去除 UNSIGNED 和 SIGNED 关键字（神通数据库不支持）
	upperType = strings.ReplaceAll(upperType, " UNSIGNED", "")
	upperType = strings.ReplaceAll(upperType, " SIGNED", "")

	// 处理带括号的类型，如 VARCHAR(255), DECIMAL(10,2)
	baseType := upperType
	size := ""
	if idx := strings.Index(upperType, "("); idx != -1 {
		baseType = upperType[:idx]
		size = upperType[idx:]
	}

	switch baseType {
	case "TINYINT":
		return "SMALLINT"
	case "SMALLINT":
		return "SMALLINT"
	case "MEDIUMINT":
		return "INTEGER"
	case "INT", "INTEGER":
		return "INTEGER"
	case "BIGINT":
		return "BIGINT"
	case "FLOAT":
		return "FLOAT"
	case "DOUBLE":
		if size != "" {
			return "DECIMAL" + size
		}
		return "DECIMAL(22,6)"
	case "DECIMAL":
		if size != "" {
			return "DECIMAL" + size
		}
		return "DECIMAL"
	case "CHAR":
		if size != "" {
			return "CHAR" + size
		}
		return "CHAR(1)"
	case "VARCHAR":
		if size != "" {
			return "VARCHAR" + size
		}
		return "VARCHAR(255)"
	case "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
		return "CLOB"
	case "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB":
		return "BLOB"
	case "DATE":
		return "DATE"
	case "DATETIME", "TIMESTAMP":
		return "TIMESTAMP"
	case "TIME":
		return "TIME"
	case "YEAR":
		return "SMALLINT"
	case "ENUM", "SET":
		// 转换为 VARCHAR
		return "VARCHAR(255)"
	case "BIT":
		return "SMALLINT"
	case "BOOLEAN", "BOOL":
		return "SMALLINT"
	case "JSON":
		return "CLOB" // Oscar 可能不支持 JSON，使用 CLOB
	default:
		// 未知类型，保持原样
		return mysqlType
	}
}

// formatDefault 格式化默认值
func (w *SchemaWriter) formatDefault(value string) string {
	// 判断是否是字符串类型
	if value == "NULL" {
		return "NULL"
	}

	// 处理 MySQL 位字面量 b'0', b'1', B'0', B'1' 等
	// 位字面量格式: b'xxx' 或 B'xxx'，其中 xxx 是二进制数字
	if matched, _ := regexp.MatchString(`^[bB]'[01]+'$`, value); matched {
		// 提取引号内的二进制值并转换为十进制整数
		bits := strings.Trim(value[2:], "'")
		if val, err := strconv.ParseInt(bits, 2, 64); err == nil {
			return strconv.FormatInt(val, 10)
		}
	}

	// 如果已经带引号，直接返回
	if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
		return value
	}

	// 如果是函数调用，不添加引号
	upper := strings.ToUpper(value)
	if upper == "CURRENT_TIMESTAMP" || upper == "NOW()" {
		return value
	}

	// 数字类型不添加引号
	if _, err := fmt.Sscanf(value, "%f", new(float64)); err == nil {
		return value
	}

	// 其他情况添加引号
	return fmt.Sprintf("'%s'", strings.ReplaceAll(value, "'", "''"))
}

// quoteIdentifier 引用标识符（转为小写）
func (w *SchemaWriter) quoteIdentifier(name string) string {
	return fmt.Sprintf(`"%s"`, strings.ToLower(name))
}

// generateIndexName 生成索引名称
// 格式：idx_表名前8字符_列名前5字符_3位随机数字
func (w *SchemaWriter) generateIndexName(tableName, columnName string) string {
	// 截取表名前8个字符
	tablePrefix := tableName
	if len(tablePrefix) > 8 {
		tablePrefix = tablePrefix[:8]
	}

	// 截取列名前5个字符
	colPrefix := columnName
	if len(colPrefix) > 5 {
		colPrefix = colPrefix[:5]
	}

	// 生成3位随机数字
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	randomNum := r.Intn(1000)

	// 转为小写
	return fmt.Sprintf("idx_%s_%s_%03d", strings.ToLower(tablePrefix), strings.ToLower(colPrefix), randomNum)
}
