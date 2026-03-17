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

// isBitType 判断列类型是否为BIT类型
func isBitType(databaseTypeName string) bool {
	return strings.ToUpper(databaseTypeName) == "BIT"
}

// convertBitToInt 将BIT类型的[]byte转换为整数
func convertBitToInt(b []byte) int64 {
	var result int64
	for _, by := range b {
		result = (result << 8) | int64(by)
	}
	return result
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

	// 记录哪些列是二进制类型或BIT类型
	isBinary := make([]bool, len(colInfos))
	isBit := make([]bool, len(colInfos))
	colNames := make([]string, len(colInfos))
	for i, col := range colInfos {
		colNames[i] = col.Name()
		isBinary[i] = isBinaryType(col.DatabaseTypeName())
		isBit[i] = isBitType(col.DatabaseTypeName())
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

		// 根据列类型决定是否转换 []byte 为 string 或整数
		for i := range values {
			if b, ok := values[i].([]byte); ok {
				// BIT类型转换为整数
				if isBit[i] {
					values[i] = convertBitToInt(b)
				} else if !isBinary[i] {
					// 只有非二进制类型才转换为字符串
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

// GetPrimaryKeyRange 获取主键范围
func (r *DataReader) GetPrimaryKeyRange(tableName, pkColumn string) (minVal, maxVal int64, err error) {
	query := fmt.Sprintf("SELECT MIN(`%s`), MAX(`%s`) FROM `%s`", pkColumn, pkColumn, tableName)

	var min, max sql.NullInt64
	err = r.client.db.QueryRow(query).Scan(&min, &max)
	if err != nil {
		return 0, 0, fmt.Errorf("获取主键范围失败: %w", err)
	}

	if !min.Valid || !max.Valid {
		// 表为空
		return 0, 0, nil
	}

	return min.Int64, max.Int64, nil
}

// ReadTableDataByRange 按主键范围读取表数据
// 用于分片并行迁移
func (r *DataReader) ReadTableDataByRange(tableName string, columns []string, pkColumn string, startID, endID int64, callback func(batch *types.DataBatch) error) error {
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

	query := fmt.Sprintf("SELECT %s FROM `%s` WHERE `%s` >= %d AND `%s` < %d",
		columnList, tableName, pkColumn, startID, pkColumn, endID)

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

	// 记录哪些列是二进制类型或BIT类型
	isBinary := make([]bool, len(colInfos))
	isBit := make([]bool, len(colInfos))
	colNames := make([]string, len(colInfos))
	for i, col := range colInfos {
		colNames[i] = col.Name()
		isBinary[i] = isBinaryType(col.DatabaseTypeName())
		isBit[i] = isBitType(col.DatabaseTypeName())
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

		// 根据列类型决定是否转换 []byte 为 string 或整数
		for i := range values {
			if b, ok := values[i].([]byte); ok {
				// BIT类型转换为整数
				if isBit[i] {
					values[i] = convertBitToInt(b)
				} else if !isBinary[i] {
					// 只有非二进制类型才转换为字符串
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

// ReadTableDataByOffset 按 OFFSET/LIMIT 读取表数据
// 用于字符串主键的分片并行迁移
func (r *DataReader) ReadTableDataByOffset(tableName string, columns []string, pkColumn string, offset, limit int64, callback func(batch *types.DataBatch) error) error {
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

	// 使用 ORDER BY + OFFSET + LIMIT 进行分片
	query := fmt.Sprintf("SELECT %s FROM `%s` ORDER BY `%s` LIMIT %d, %d",
		columnList, tableName, pkColumn, offset, limit)

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

	// 记录哪些列是二进制类型或BIT类型
	isBinary := make([]bool, len(colInfos))
	isBit := make([]bool, len(colInfos))
	colNames := make([]string, len(colInfos))
	for i, col := range colInfos {
		colNames[i] = col.Name()
		isBinary[i] = isBinaryType(col.DatabaseTypeName())
		isBit[i] = isBitType(col.DatabaseTypeName())
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

		// 根据列类型决定是否转换 []byte 为 string 或整数
		for i := range values {
			if b, ok := values[i].([]byte); ok {
				// BIT类型转换为整数
				if isBit[i] {
					values[i] = convertBitToInt(b)
				} else if !isBinary[i] {
					// 只有非二进制类型才转换为字符串
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
