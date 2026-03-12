package mysql

import (
	"database/sql"
	"fmt"
	"strings"

	"mysql2oscar/pkg/types"
)

// SchemaReader 表结构读取器
type SchemaReader struct {
	client *Client
}

// NewSchemaReader 创建表结构读取器
func NewSchemaReader(client *Client) *SchemaReader {
	return &SchemaReader{client: client}
}

// GetTables 获取所有表名
func (r *SchemaReader) GetTables() ([]string, error) {
	query := `
		SELECT TABLE_NAME
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
		ORDER BY TABLE_NAME
	`

	rows, err := r.client.db.Query(query, r.client.dbName)
	if err != nil {
		return nil, fmt.Errorf("查询表列表失败: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("扫描表名失败: %w", err)
		}
		tables = append(tables, name)
	}

	return tables, nil
}

// GetTableSchema 获取表结构
func (r *SchemaReader) GetTableSchema(tableName string) (*types.Table, error) {
	table := &types.Table{
		Schema: r.client.dbName,
		Name:   tableName,
	}

	// 获取列信息
	columns, err := r.getColumns(tableName)
	if err != nil {
		return nil, err
	}
	table.Columns = columns

	// 获取索引信息
	indexes, err := r.getIndexes(tableName)
	if err != nil {
		return nil, err
	}
	table.Indexes = indexes

	// 获取外键信息
	fks, err := r.getForeignKeys(tableName)
	if err != nil {
		return nil, err
	}
	table.ForeignKeys = fks

	// 获取表注释
	comment, err := r.getTableComment(tableName)
	if err != nil {
		return nil, err
	}
	table.Comment = comment

	return table, nil
}

// getColumns 获取列信息
func (r *SchemaReader) getColumns(tableName string) ([]types.Column, error) {
	query := `
		SELECT
			COLUMN_NAME,
			COLUMN_TYPE,
			IS_NULLABLE,
			COLUMN_DEFAULT,
			COLUMN_KEY,
			EXTRA,
			COLUMN_COMMENT,
			CHARACTER_MAXIMUM_LENGTH,
			NUMERIC_PRECISION,
			NUMERIC_SCALE
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`

	rows, err := r.client.db.Query(query, r.client.dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询列信息失败: %w", err)
	}
	defer rows.Close()

	var columns []types.Column
	for rows.Next() {
		var col types.Column
		var isNullable, columnKey, extra string
		var defaultVal sql.NullString
		var charMaxLen, numPrecision, numScale sql.NullInt64

		err := rows.Scan(
			&col.Name,
			&col.DataType,
			&isNullable,
			&defaultVal,
			&columnKey,
			&extra,
			&col.Comment,
			&charMaxLen,
			&numPrecision,
			&numScale,
		)
		if err != nil {
			return nil, fmt.Errorf("扫描列信息失败: %w", err)
		}

		col.IsNullable = isNullable == "YES"
		if defaultVal.Valid {
			col.DefaultValue = &defaultVal.String
		}
		col.IsPrimary = columnKey == "PRI"
		col.IsAutoIncr = strings.Contains(extra, "auto_increment")

		if charMaxLen.Valid {
			col.CharMaxLength = int(charMaxLen.Int64)
		}
		if numPrecision.Valid {
			col.NumericPrecision = int(numPrecision.Int64)
		}
		if numScale.Valid {
			col.NumericScale = int(numScale.Int64)
		}

		// 解析 ENUM 类型
		if strings.HasPrefix(col.DataType, "enum") || strings.HasPrefix(col.DataType, "set") {
			col.EnumValues = parseEnumValues(col.DataType)
		}

		columns = append(columns, col)
	}

	// 如果没有列，返回错误
	if len(columns) == 0 {
		return nil, fmt.Errorf("表 %s 在数据库 %s 中不存在或没有列信息", tableName, r.client.dbName)
	}

	return columns, nil
}

// getIndexes 获取索引信息
func (r *SchemaReader) getIndexes(tableName string) ([]types.Index, error) {
	query := `
		SELECT
			INDEX_NAME,
			COLUMN_NAME,
			NON_UNIQUE
		FROM INFORMATION_SCHEMA.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY INDEX_NAME, SEQ_IN_INDEX
	`

	rows, err := r.client.db.Query(query, r.client.dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询索引信息失败: %w", err)
	}
	defer rows.Close()

	indexMap := make(map[string]*types.Index)
	for rows.Next() {
		var indexName, columnName string
		var nonUnique bool

		if err := rows.Scan(&indexName, &columnName, &nonUnique); err != nil {
			return nil, fmt.Errorf("扫描索引信息失败: %w", err)
		}

		if idx, exists := indexMap[indexName]; exists {
			idx.Columns = append(idx.Columns, columnName)
		} else {
			indexMap[indexName] = &types.Index{
				Name:      indexName,
				Columns:   []string{columnName},
				IsUnique:  !nonUnique,
				IsPrimary: indexName == "PRIMARY",
			}
		}
	}

	var indexes []types.Index
	for _, idx := range indexMap {
		indexes = append(indexes, *idx)
	}

	return indexes, nil
}

// getForeignKeys 获取外键信息
func (r *SchemaReader) getForeignKeys(tableName string) ([]types.ForeignKey, error) {
	query := `
		SELECT
			kcu.CONSTRAINT_NAME,
			kcu.COLUMN_NAME,
			kcu.REFERENCED_TABLE_NAME,
			kcu.REFERENCED_COLUMN_NAME,
			COALESCE(rc.DELETE_RULE, 'NO ACTION'),
			COALESCE(rc.UPDATE_RULE, 'NO ACTION')
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
		LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
			ON kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
			AND kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
		WHERE kcu.TABLE_SCHEMA = ? AND kcu.TABLE_NAME = ? AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
	`

	rows, err := r.client.db.Query(query, r.client.dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询外键信息失败: %w", err)
	}
	defer rows.Close()

	fkMap := make(map[string]*types.ForeignKey)
	for rows.Next() {
		var constraintName, columnName, refTable, refColumn, deleteRule, updateRule string

		if err := rows.Scan(&constraintName, &columnName, &refTable, &refColumn, &deleteRule, &updateRule); err != nil {
			return nil, fmt.Errorf("扫描外键信息失败: %w", err)
		}

		if fk, exists := fkMap[constraintName]; exists {
			fk.Columns = append(fk.Columns, columnName)
			fk.ReferencedColumns = append(fk.ReferencedColumns, refColumn)
		} else {
			fkMap[constraintName] = &types.ForeignKey{
				Name:              constraintName,
				Columns:           []string{columnName},
				ReferencedTable:   refTable,
				ReferencedColumns: []string{refColumn},
				OnDelete:          deleteRule,
				OnUpdate:          updateRule,
			}
		}
	}

	var fks []types.ForeignKey
	for _, fk := range fkMap {
		fks = append(fks, *fk)
	}

	return fks, nil
}

// getTableComment 获取表注释
func (r *SchemaReader) getTableComment(tableName string) (string, error) {
	query := `
		SELECT TABLE_COMMENT
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`

	var comment string
	var nullComment sql.NullString
	err := r.client.db.QueryRow(query, r.client.dbName, tableName).Scan(&nullComment)
	if err != nil {
		// 如果查询失败，返回空注释而不是错误
		return "", nil
	}
	if nullComment.Valid {
		comment = nullComment.String
	}

	return comment, nil
}

// parseEnumValues 解析 ENUM/SET 值
func parseEnumValues(dataType string) []string {
	// 简单解析: enum('a','b','c') -> ['a','b','c']
	start := strings.Index(dataType, "(")
	end := strings.LastIndex(dataType, ")")
	if start == -1 || end == -1 {
		return nil
	}

	values := strings.Split(dataType[start+1:end], ",")
	result := make([]string, 0, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		v = strings.Trim(v, "'\"")
		result = append(result, v)
	}
	return result
}

// GetViews 获取所有视图名
func (r *SchemaReader) GetViews() ([]string, error) {
	query := `
		SELECT TABLE_NAME
		FROM INFORMATION_SCHEMA.VIEWS
		WHERE TABLE_SCHEMA = ?
		ORDER BY TABLE_NAME
	`

	rows, err := r.client.db.Query(query, r.client.dbName)
	if err != nil {
		return nil, fmt.Errorf("查询视图列表失败: %w", err)
	}
	defer rows.Close()

	var views []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("扫描视图名失败: %w", err)
		}
		views = append(views, name)
	}

	return views, nil
}

// GetViewDefinition 获取视图定义
func (r *SchemaReader) GetViewDefinition(viewName string) (*types.View, error) {
	query := `
		SELECT VIEW_DEFINITION
		FROM INFORMATION_SCHEMA.VIEWS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`

	var definition string
	err := r.client.db.QueryRow(query, r.client.dbName, viewName).Scan(&definition)
	if err != nil {
		return nil, fmt.Errorf("查询视图定义失败: %w", err)
	}

	return &types.View{
		Schema:     r.client.dbName,
		Name:       viewName,
		Definition: definition,
	}, nil
}

// AutoIncrementInfo 自增列信息（包含起始值）
type AutoIncrementInfo struct {
	TableName     string
	ColumnName    string
	AutoIncrement int64
}

// GetAutoIncrementInfo 批量获取所有表的自增列信息
// 返回：表名 -> 自增列信息列表
func (r *SchemaReader) GetAutoIncrementInfo() (map[string]AutoIncrementInfo, error) {
	// 查询MySQL自增列信息，包括Auto_increment起始值
	query := `
		SELECT
			a.TABLE_NAME,
			b.COLUMN_NAME,
			a.Auto_increment
		FROM (
			SELECT TABLE_NAME, Auto_increment
			FROM INFORMATION_SCHEMA.TABLES
			WHERE TABLE_SCHEMA = ? AND AUTO_INCREMENT IS NOT NULL
		) a
		JOIN (
			SELECT TABLE_NAME, COLUMN_NAME
			FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_SCHEMA = ? AND EXTRA = 'auto_increment'
		) b ON a.TABLE_NAME = b.TABLE_NAME
	`

	rows, err := r.client.db.Query(query, r.client.dbName, r.client.dbName)
	if err != nil {
		return nil, fmt.Errorf("查询自增列信息失败: %w", err)
	}
	defer rows.Close()

	result := make(map[string]AutoIncrementInfo)
	for rows.Next() {
		var tableName, columnName string
		var autoIncrement sql.NullInt64

		if err := rows.Scan(&tableName, &columnName, &autoIncrement); err != nil {
			return nil, fmt.Errorf("扫描自增列信息失败: %w", err)
		}

		info := AutoIncrementInfo{
			TableName:  tableName,
			ColumnName: columnName,
		}
		if autoIncrement.Valid {
			info.AutoIncrement = autoIncrement.Int64
		} else {
			info.AutoIncrement = 1 // 默认从1开始
		}

		result[tableName] = info
	}

	return result, nil
}
