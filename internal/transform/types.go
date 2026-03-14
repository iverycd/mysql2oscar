package transform

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"mysql2oscar/pkg/types"
)

// TypeMapper 类型映射器
type TypeMapper struct {
	// 自定义类型映射
	customMappings map[string]string
}

// NewTypeMapper 创建类型映射器
func NewTypeMapper() *TypeMapper {
	return &TypeMapper{
		customMappings: make(map[string]string),
	}
}

// SetCustomMapping 设置自定义类型映射
func (m *TypeMapper) SetCustomMapping(mysqlType, oscarType string) {
	m.customMappings[strings.ToUpper(mysqlType)] = oscarType
}

// MapType 将 MySQL 类型映射到 Oscar 类型
func (m *TypeMapper) MapType(mysqlType string) string {
	upperType := strings.ToUpper(mysqlType)

	// 去除 UNSIGNED 和 SIGNED 关键字（神通数据库不支持）
	upperType = strings.ReplaceAll(upperType, " UNSIGNED", "")
	upperType = strings.ReplaceAll(upperType, " SIGNED", "")

	// 检查自定义映射
	if oscarType, ok := m.customMappings[upperType]; ok {
		return oscarType
	}

	// 处理带括号的类型
	baseType := upperType
	size := ""
	if idx := strings.Index(upperType, "("); idx != -1 {
		baseType = upperType[:idx]
		size = upperType[idx:]
	}

	// 标准类型映射
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
	case "NUMERIC":
		if size != "" {
			return "NUMERIC" + size
		}
		return "NUMERIC"
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
	case "BINARY", "VARBINARY":
		return "BLOB"
	case "DATE":
		return "DATE"
	case "DATETIME":
		return "TIMESTAMP"
	case "TIMESTAMP":
		return "TIMESTAMP"
	case "TIME":
		return "TIME"
	case "YEAR":
		return "SMALLINT"
	case "ENUM", "SET":
		return "VARCHAR(255)"
	case "BIT":
		if size != "" {
			return "SMALLINT"
		}
		return "SMALLINT"
	case "BOOLEAN", "BOOL":
		return "SMALLINT"
	case "JSON":
		return "CLOB"
	default:
		return mysqlType
	}
}

// TransformTable 转换表结构
func (m *TypeMapper) TransformTable(table *types.Table) *types.Table {
	transformed := &types.Table{
		Schema:      table.Schema,
		Name:        table.Name,
		Comment:     table.Comment,
		Columns:     make([]types.Column, len(table.Columns)),
		Indexes:     make([]types.Index, len(table.Indexes)),
		ForeignKeys: make([]types.ForeignKey, len(table.ForeignKeys)),
	}

	// 转换列类型
	for i, col := range table.Columns {
		transformed.Columns[i] = col
		transformed.Columns[i].DataType = m.MapType(col.DataType)
	}

	// 复制索引
	copy(transformed.Indexes, table.Indexes)

	// 复制外键
	copy(transformed.ForeignKeys, table.ForeignKeys)

	return transformed
}

// DDLConverter DDL 转换器
type DDLConverter struct {
	typeMapper *TypeMapper
}

// NewDDLConverter 创建 DDL 转换器
func NewDDLConverter(typeMapper *TypeMapper) *DDLConverter {
	return &DDLConverter{
		typeMapper: typeMapper,
	}
}

// GenerateCreateTableSQL 生成建表 SQL
func (c *DDLConverter) GenerateCreateTableSQL(table *types.Table) string {
	var sql strings.Builder

	sql.WriteString(fmt.Sprintf("CREATE TABLE \"%s\" (\n", table.Name))

	// 列定义
	for i, col := range table.Columns {
		sql.WriteString(fmt.Sprintf("  \"%s\" %s", col.Name, c.typeMapper.MapType(col.DataType)))

		if !col.IsNullable {
			sql.WriteString(" NOT NULL")
		}

		if col.IsAutoIncr {
			sql.WriteString(" AUTO_INCREMENT")
		}

		if col.DefaultValue != nil && !col.IsAutoIncr {
			sql.WriteString(fmt.Sprintf(" DEFAULT %s", c.formatDefault(*col.DefaultValue)))
		}

		if i < len(table.Columns)-1 {
			sql.WriteString(",\n")
		}
	}

	// 主键
	for _, idx := range table.Indexes {
		if idx.IsPrimary {
			cols := make([]string, len(idx.Columns))
			for i, col := range idx.Columns {
				cols[i] = fmt.Sprintf("\"%s\"", col)
			}
			sql.WriteString(fmt.Sprintf(",\n  PRIMARY KEY (%s)", strings.Join(cols, ", ")))
		}
	}

	sql.WriteString("\n)")

	if table.Comment != "" {
		sql.WriteString(fmt.Sprintf(" COMMENT '%s'", table.Comment))
	}

	return sql.String()
}

// GenerateCreateIndexSQL 生成创建索引 SQL
func (c *DDLConverter) GenerateCreateIndexSQL(tableName string, index types.Index) string {
	cols := make([]string, len(index.Columns))
	for i, col := range index.Columns {
		cols[i] = fmt.Sprintf("\"%s\"", col)
	}

	indexType := "INDEX"
	if index.IsUnique {
		indexType = "UNIQUE INDEX"
	}

	return fmt.Sprintf("CREATE %s \"%s\" ON \"%s\" (%s)",
		indexType, index.Name, tableName, strings.Join(cols, ", "))
}

// GenerateCreateViewSQL 生成创建视图 SQL
func (c *DDLConverter) GenerateCreateViewSQL(view *types.View) string {
	return fmt.Sprintf("CREATE VIEW \"%s\" AS %s", view.Name, view.Definition)
}

// formatDefault 格式化默认值
func (c *DDLConverter) formatDefault(value string) string {
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

	if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
		return value
	}

	upper := strings.ToUpper(value)
	if upper == "CURRENT_TIMESTAMP" || upper == "NOW()" {
		return value
	}

	// 数字类型
	if _, err := fmt.Sscanf(value, "%f", new(float64)); err == nil {
		return value
	}

	return fmt.Sprintf("'%s'", strings.ReplaceAll(value, "'", "''"))
}
