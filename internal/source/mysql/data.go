package mysql

import (
	"database/sql"
	"fmt"
	"strings"

	"mysql2oscar/pkg/types"
)

// DataReader 数据读取器
type DataReader struct {
	client    *Client
	batchSize int
}

// NewDataReader 创建数据读取器
func NewDataReader(client *Client, batchSize int) *DataReader {
	return &DataReader{
		client:    client,
		batchSize: batchSize,
	}
}

// isBinaryType 判断列类型是否为二进制类型（BLOB等）
func isBinaryType(databaseTypeName string) bool {
	upper := strings.ToUpper(databaseTypeName)
	switch upper {
	case "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "BINARY", "VARBINARY":
		return true
	default:
		return false
	}
}

// ReadTableData 流式读取表数据
// 使用回调函数处理每一批数据
func (r *DataReader) ReadTableData(tableName string, columns []string, callback func(batch *types.DataBatch) error) error {
	// 构建查询
	columnList := "*"
	if len(columns) > 0 {
		columnList = ""
		for i, col := range columns {
			if i > 0 {
				columnList += ", "
			}
			columnList += fmt.Sprintf("`%s`", col)
		}
	}

	query := fmt.Sprintf("SELECT %s FROM `%s`", columnList, tableName)

	rows, err := r.client.db.Query(query)
	if err != nil {
		return fmt.Errorf("查询数据失败: %w", err)
	}
	defer rows.Close()

	// 获取列信息
	colInfos, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("获取列类型失败: %w", err)
	}

	// 记录哪些列是二进制类型
	isBinary := make([]bool, len(colInfos))
	colNames := make([]string, len(colInfos))
	for i, col := range colInfos {
		colNames[i] = col.Name()
		isBinary[i] = isBinaryType(col.DatabaseTypeName())
	}

	batch := &types.DataBatch{
		Columns: colNames,
		Rows:    make([]types.DataRow, 0, r.batchSize),
	}

	for rows.Next() {
		// 创建扫描目标
		values := make([]interface{}, len(colInfos))
		valuePtrs := make([]interface{}, len(colInfos))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("扫描数据行失败: %w", err)
		}

		// 根据列类型决定是否转换 []byte 为 string
		for i := range values {
			if b, ok := values[i].([]byte); ok {
				// 只有非二进制类型才转换为字符串
				if !isBinary[i] {
					values[i] = string(b)
				}
				// 二进制类型保持 []byte 不变
			}
		}

		batch.Rows = append(batch.Rows, types.DataRow{Values: values})

		// 达到批处理大小，回调处理
		if len(batch.Rows) >= r.batchSize {
			if err := callback(batch); err != nil {
				return err
			}
			batch.Rows = make([]types.DataRow, 0, r.batchSize)
		}
	}

	// 处理剩余数据
	if len(batch.Rows) > 0 {
		if err := callback(batch); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("读取数据失败: %w", err)
	}

	return nil
}

// GetRowCount 获取表行数
func (r *DataReader) GetRowCount(tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)

	var count int64
	err := r.client.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("获取行数失败: %w", err)
	}

	return count, nil
}

// GetColumnNames 获取表的所有列名
func (r *DataReader) GetColumnNames(tableName string) ([]string, error) {
	query := `
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`

	rows, err := r.client.db.Query(query, r.client.dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询列名失败: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("扫描列名失败: %w", err)
		}
		columns = append(columns, name)
	}

	return columns, nil
}

// StreamData 使用游标流式读取大数据
func (r *DataReader) StreamData(tableName string, callback func(row map[string]interface{}) error) error {
	query := fmt.Sprintf("SELECT * FROM `%s`", tableName)

	rows, err := r.client.db.Query(query)
	if err != nil {
		return fmt.Errorf("查询数据失败: %w", err)
	}
	defer rows.Close()

	colInfos, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("获取列类型失败: %w", err)
	}

	colNames := make([]string, len(colInfos))
	for i, col := range colInfos {
		colNames[i] = col.Name()
	}

	for rows.Next() {
		values := make([]interface{}, len(colInfos))
		valuePtrs := make([]interface{}, len(colInfos))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("扫描数据行失败: %w", err)
		}

		// 构建 map
		row := make(map[string]interface{})
		for i, name := range colNames {
			val := values[i]
			// 处理 NULL 值
			if b, ok := val.([]byte); ok {
				row[name] = string(b)
			} else {
				row[name] = val
			}
		}

		if err := callback(row); err != nil {
			return err
		}
	}

	return rows.Err()
}

// ReadWithOffset 带偏移量读取数据（用于分页）
func (r *DataReader) ReadWithOffset(tableName string, offset, limit int64) (*types.DataBatch, error) {
	query := fmt.Sprintf("SELECT * FROM `%s` LIMIT %d OFFSET %d", tableName, limit, offset)

	rows, err := r.client.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("查询数据失败: %w", err)
	}
	defer rows.Close()

	colInfos, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("获取列类型失败: %w", err)
	}

	colNames := make([]string, len(colInfos))
	for i, col := range colInfos {
		colNames[i] = col.Name()
	}

	batch := &types.DataBatch{
		Columns: colNames,
		Rows:    make([]types.DataRow, 0, limit),
	}

	for rows.Next() {
		values := make([]interface{}, len(colInfos))
		valuePtrs := make([]interface{}, len(colInfos))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("扫描数据行失败: %w", err)
		}

		batch.Rows = append(batch.Rows, types.DataRow{Values: values})
	}

	return batch, rows.Err()
}

// PrimaryKeyInfo 主键信息
type PrimaryKeyInfo struct {
	Name      string               // 主键列名
	Type      types.PrimaryKeyType // 主键类型
	MinValue  interface{}          // 最小值（仅整数主键有效）
	MaxValue  interface{}          // 最大值（仅整数主键有效）
	IsNumeric bool                 // 是否是数值类型
}

// GetPrimaryKeyInfo 获取表的主键信息
func (r *DataReader) GetPrimaryKeyInfo(tableName string) (*PrimaryKeyInfo, error) {
	// 查询主键列名和类型
	query := `
		SELECT k.COLUMN_NAME, c.DATA_TYPE, c.COLUMN_TYPE
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
		JOIN INFORMATION_SCHEMA.COLUMNS c
			ON k.TABLE_SCHEMA = c.TABLE_SCHEMA
			AND k.TABLE_NAME = c.TABLE_NAME
			AND k.COLUMN_NAME = c.COLUMN_NAME
		WHERE k.TABLE_SCHEMA = ? AND k.TABLE_NAME = ?
			AND k.CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY k.ORDINAL_POSITION
		LIMIT 1
	`

	var pkName, dataType, columnType string
	err := r.client.db.QueryRow(query, r.client.dbName, tableName).Scan(&pkName, &dataType, &columnType)
	if err != nil {
		// 无主键
		return &PrimaryKeyInfo{Type: types.PKTypeNone}, nil
	}

	// 判断是否是整数类型
	isInteger := isIntegerType(dataType)

	if !isInteger {
		// UUID 或其他类型
		return &PrimaryKeyInfo{
			Name: pkName,
			Type: types.PKTypeOther,
		}, nil
	}

	// 整数主键，获取最小值和最大值
	minMaxQuery := fmt.Sprintf("SELECT MIN(`%s`), MAX(`%s`) FROM `%s`", pkName, pkName, tableName)
	var minValue, maxValue interface{}
	err = r.client.db.QueryRow(minMaxQuery).Scan(&minValue, &maxValue)
	if err != nil {
		return nil, fmt.Errorf("获取主键范围失败: %w", err)
	}

	return &PrimaryKeyInfo{
		Name:      pkName,
		Type:      types.PKTypeInteger,
		MinValue:  minValue,
		MaxValue:  maxValue,
		IsNumeric: true,
	}, nil
}

// isIntegerType 判断是否是整数类型
func isIntegerType(dataType string) bool {
	switch strings.ToUpper(dataType) {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT":
		return true
	default:
		return false
	}
}

// ReadTableDataByRange 按主键范围读取数据（用于整数主键分片）
func (r *DataReader) ReadTableDataByRange(
	tableName string,
	columns []string,
	pkName string,
	startValue, endValue interface{},
	callback func(batch *types.DataBatch) error,
) error {
	// 构建列列表
	columnList := "*"
	if len(columns) > 0 {
		columnList = ""
		for i, col := range columns {
			if i > 0 {
				columnList += ", "
			}
			columnList += fmt.Sprintf("`%s`", col)
		}
	}

	// 构建范围查询
	var query string
	var rows *sql.Rows
	var err error

	if endValue == nil {
		// 最后一个分片，无上界
		query = fmt.Sprintf("SELECT %s FROM `%s` WHERE `%s` >= ?", columnList, tableName, pkName)
		rows, err = r.client.db.Query(query, startValue)
	} else {
		query = fmt.Sprintf("SELECT %s FROM `%s` WHERE `%s` >= ? AND `%s` < ?", columnList, tableName, pkName, pkName)
		rows, err = r.client.db.Query(query, startValue, endValue)
	}
	if err != nil {
		return fmt.Errorf("查询数据失败: %w", err)
	}
	defer rows.Close()

	// 获取列信息
	colInfos, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("获取列类型失败: %w", err)
	}

	// 记录哪些列是二进制类型
	isBinary := make([]bool, len(colInfos))
	colNames := make([]string, len(colInfos))
	for i, col := range colInfos {
		colNames[i] = col.Name()
		isBinary[i] = isBinaryType(col.DatabaseTypeName())
	}

	batch := &types.DataBatch{
		Columns: colNames,
		Rows:    make([]types.DataRow, 0, r.batchSize),
	}

	for rows.Next() {
		values := make([]interface{}, len(colInfos))
		valuePtrs := make([]interface{}, len(colInfos))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("扫描数据行失败: %w", err)
		}

		// 根据列类型决定是否转换 []byte 为 string
		for i := range values {
			if b, ok := values[i].([]byte); ok {
				if !isBinary[i] {
					values[i] = string(b)
				}
			}
		}

		batch.Rows = append(batch.Rows, types.DataRow{Values: values})

		// 达到批处理大小，回调处理
		if len(batch.Rows) >= r.batchSize {
			if err := callback(batch); err != nil {
				return err
			}
			batch.Rows = make([]types.DataRow, 0, r.batchSize)
		}
	}

	// 处理剩余数据
	if len(batch.Rows) > 0 {
		if err := callback(batch); err != nil {
			return err
		}
	}

	return rows.Err()
}

// ReadTableDataByOffset 按偏移量读取数据（用于非整数主键分片）
func (r *DataReader) ReadTableDataByOffset(
	tableName string,
	offset, limit int64,
	callback func(batch *types.DataBatch) error,
) error {
	batch, err := r.ReadWithOffset(tableName, offset, limit)
	if err != nil {
		return err
	}

	if len(batch.Rows) > 0 {
		return callback(batch)
	}
	return nil
}

// CalculateShards 计算分片范围
func (r *DataReader) CalculateShards(
	totalRows int64,
	pkInfo *PrimaryKeyInfo,
	shardSize int64,
) []types.ShardRange {
	var shards []types.ShardRange

	if pkInfo.Type == types.PKTypeInteger && pkInfo.MinValue != nil && pkInfo.MaxValue != nil {
		// 整数主键：按值范围分片
		minVal := toInt64(pkInfo.MinValue)
		maxVal := toInt64(pkInfo.MaxValue)

		shardIndex := 0
		for start := minVal; start <= maxVal; start += shardSize {
			end := start + shardSize
			if end > maxVal {
				end = maxVal + 1 // 确保包含最大值
			}
			shards = append(shards, types.ShardRange{
				StartValue: start,
				EndValue:   end,
				ShardIndex: shardIndex,
			})
			shardIndex++
		}
	} else {
		// 非整数主键：按行数 OFFSET 分片
		shardIndex := 0
		for offset := int64(0); offset < totalRows; offset += shardSize {
			limit := shardSize
			if offset+limit > totalRows {
				limit = totalRows - offset
			}
			shards = append(shards, types.ShardRange{
				Offset:     offset,
				Limit:      limit,
				ShardIndex: shardIndex,
			})
			shardIndex++
		}
	}

	return shards
}

// toInt64 将 interface{} 转换为 int64
func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int32:
		return int64(val)
	case int:
		return int64(val)
	case uint64:
		return int64(val)
	case uint32:
		return int64(val)
	case uint:
		return int64(val)
	case float64:
		return int64(val)
	case float32:
		return int64(val)
	default:
		return 0
	}
}
