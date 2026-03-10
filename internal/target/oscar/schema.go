package oscar

import (
	"fmt"
	"strings"

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

// CreateTable 创建表（包含自增属性，用于普通数据库）
func (w *SchemaWriter) CreateTable(table *types.Table) error {
	// 检查列是否为空
	if len(table.Columns) == 0 {
		return fmt.Errorf("创建表 %s 失败: 表没有定义任何列", table.Name)
	}

	// 先删除已存在的表
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", w.quoteIdentifier(table.Name))
	if _, err := w.client.Exec(dropSQL); err != nil {
		return fmt.Errorf("删除已存在的表失败: %w", err)
	}

	var sql strings.Builder

	// 构建建表语句
	sql.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", w.quoteIdentifier(table.Name)))

	// 列定义
	for i, col := range table.Columns {
		sql.WriteString(fmt.Sprintf("  %s %s", w.quoteIdentifier(col.Name), w.convertType(col.DataType)))

		if !col.IsNullable {
			sql.WriteString(" NOT NULL")
		}

		if col.IsAutoIncr {
			sql.WriteString(" AUTO_INCREMENT")
		}

		if col.DefaultValue != nil && !col.IsAutoIncr {
			sql.WriteString(fmt.Sprintf(" DEFAULT %s", w.formatDefault(*col.DefaultValue)))
		}

		if col.Comment != "" {
			sql.WriteString(fmt.Sprintf(" COMMENT '%s'", col.Comment))
		}

		if i < len(table.Columns)-1 {
			sql.WriteString(",\n")
		}
	}

	// 主键
	for _, idx := range table.Indexes {
		if idx.IsPrimary {
			cols := make([]string, len(idx.Columns))
			for i, c := range idx.Columns {
				cols[i] = w.quoteIdentifier(c)
			}
			sql.WriteString(fmt.Sprintf(",\n  PRIMARY KEY (%s)", strings.Join(cols, ", ")))
		}
	}

	sql.WriteString("\n)")

	// 表注释
	if table.Comment != "" {
		sql.WriteString(fmt.Sprintf(" COMMENT '%s'", table.Comment))
	}

	// 执行建表语句
	_, err := w.client.Exec(sql.String())
	if err != nil {
		return fmt.Errorf("创建表 %s 失败: %w", table.Name, err)
	}

	return nil
}

// CreateTableWithoutAutoIncr 创建表但不包含自增属性（用于神通数据库）
// 神通数据库需要先创建表，再创建唯一索引，最后设置自增属性
func (w *SchemaWriter) CreateTableWithoutAutoIncr(table *types.Table) error {
	// 检查列是否为空
	if len(table.Columns) == 0 {
		return fmt.Errorf("创建表 %s 失败: 表没有定义任何列", table.Name)
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

		if col.Comment != "" {
			sql.WriteString(fmt.Sprintf(" COMMENT '%s'", col.Comment))
		}

		if i < len(table.Columns)-1 {
			sql.WriteString(",\n")
		}
	}

	// 主键
	for _, idx := range table.Indexes {
		if idx.IsPrimary {
			cols := make([]string, len(idx.Columns))
			for i, c := range idx.Columns {
				cols[i] = w.quoteIdentifier(c)
			}
			sql.WriteString(fmt.Sprintf(",\n  PRIMARY KEY (%s)", strings.Join(cols, ", ")))
		}
	}

	sql.WriteString("\n)")

	// 表注释
	if table.Comment != "" {
		sql.WriteString(fmt.Sprintf(" COMMENT '%s'", table.Comment))
	}

	// 执行建表语句
	_, err := w.client.Exec(sql.String())
	if err != nil {
		return fmt.Errorf("创建表 %s 失败: %w", table.Name, err)
	}

	return nil
}

// CreateAutoIncrUniqueIndex 为自增列创建唯一索引
func (w *SchemaWriter) CreateAutoIncrUniqueIndex(tableName, columnName string) error {
	indexName := fmt.Sprintf("UK_%s_%s", tableName, columnName)
	sql := fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s (%s)",
		w.quoteIdentifier(indexName),
		w.quoteIdentifier(tableName),
		w.quoteIdentifier(columnName))

	if _, err := w.client.Exec(sql); err != nil {
		return fmt.Errorf("创建自增列唯一索引失败: %w", err)
	}

	return nil
}

// SetColumnAutoIncrement 设置列为自增列
func (w *SchemaWriter) SetColumnAutoIncrement(tableName, columnName string) error {
	// 神通数据库语法：ALTER TABLE 表名 ALTER TYPE 列名 INT AUTO_INCREMENT
	sql := fmt.Sprintf("ALTER TABLE %s ALTER TYPE %s INT AUTO_INCREMENT",
		w.quoteIdentifier(tableName),
		w.quoteIdentifier(columnName))

	if _, err := w.client.Exec(sql); err != nil {
		return fmt.Errorf("设置自增属性失败: %w", err)
	}

	return nil
}

// CreateIndexes 创建索引
func (w *SchemaWriter) CreateIndexes(tableName string, indexes []types.Index) error {
	for _, idx := range indexes {
		if idx.IsPrimary {
			continue // 主键在建表时已创建
		}

		cols := make([]string, len(idx.Columns))
		for i, c := range idx.Columns {
			cols[i] = w.quoteIdentifier(c)
		}

		indexType := "INDEX"
		if idx.IsUnique {
			indexType = "UNIQUE INDEX"
		}

		sql := fmt.Sprintf("CREATE %s %s ON %s (%s)",
			indexType,
			w.quoteIdentifier(idx.Name),
			w.quoteIdentifier(tableName),
			strings.Join(cols, ", "))

		if _, err := w.client.Exec(sql); err != nil {
			return fmt.Errorf("创建索引 %s 失败: %w", idx.Name, err)
		}
	}

	return nil
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
func (w *SchemaWriter) CreateSingleForeignKey(tableName string, fk types.ForeignKey) error {
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

	return nil
}

// CreateView 创建视图
func (w *SchemaWriter) CreateView(view *types.View) error {
	sql := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s",
		w.quoteIdentifier(view.Name),
		view.Definition)

	if _, err := w.client.Exec(sql); err != nil {
		return fmt.Errorf("创建视图 %s 失败: %w", view.Name, err)
	}

	return nil
}

// convertType 将 MySQL 类型转换为 Oscar 类型
func (w *SchemaWriter) convertType(mysqlType string) string {
	upperType := strings.ToUpper(mysqlType)

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
		return "DOUBLE"
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

// quoteIdentifier 引用标识符
func (w *SchemaWriter) quoteIdentifier(name string) string {
	return fmt.Sprintf(`"%s"`, name)
}
