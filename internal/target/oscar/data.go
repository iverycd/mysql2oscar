package oscar

import (
	"fmt"
	"strings"
	"time"

	"mysql2oscar/pkg/types"
)

// DataWriter 数据写入器
type DataWriter struct {
	client *Client
}

// NewDataWriter 创建数据写入器
func NewDataWriter(client *Client) *DataWriter {
	return &DataWriter{client: client}
}

// InsertBatch 批量插入数据
func (w *DataWriter) InsertBatch(tableName string, batch *types.DataBatch) (int64, error) {
	if len(batch.Rows) == 0 {
		return 0, nil
	}

	// 构建插入语句
	quotedCols := make([]string, len(batch.Columns))
	for i, col := range batch.Columns {
		quotedCols[i] = w.quoteIdentifier(col)
	}

	// 构建占位符 (Oscar 使用 $1, $2, $3 )
	placeholders := make([]string, len(batch.Columns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}

	// 单条插入 SQL
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		w.quoteIdentifier(tableName),
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "))

	// 使用事务批量插入
	tx, err := w.client.Begin()
	if err != nil {
		return 0, fmt.Errorf("开始事务失败: %w", err)
	}

	var inserted int64
	for _, row := range batch.Rows {
		result, err := tx.Exec(sql, row.Values...)
		if err != nil {
			tx.Rollback()
			// 输出完整的INSERT语句
			fullSQL := w.formatInsertSQL(tableName, batch.Columns, []types.DataRow{row})
			return inserted, fmt.Errorf("插入数据失败: %w\n完整INSERT语句:\n%s", err, fullSQL)
		}

		affected, _ := result.RowsAffected()
		inserted += affected
	}

	if err := tx.Commit(); err != nil {
		return inserted, fmt.Errorf("提交事务失败: %w", err)
	}

	return inserted, nil
}

// InsertBatchOptimized 优化的批量插入（使用批量 INSERT 语句）
func (w *DataWriter) InsertBatchOptimized(tableName string, batch *types.DataBatch) (int64, error) {
	if len(batch.Rows) == 0 {
		return 0, nil
	}

	// 构建列名部分
	quotedCols := make([]string, len(batch.Columns))
	for i, col := range batch.Columns {
		quotedCols[i] = w.quoteIdentifier(col)
	}

	// 构建批量插入 SQL
	var sql strings.Builder
	sql.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ",
		w.quoteIdentifier(tableName),
		strings.Join(quotedCols, ", ")))

	// 构建占位符并收集所有值
	allValues := make([]interface{}, 0, len(batch.Rows)*len(batch.Columns))
	placeholderIdx := 1

	for i, row := range batch.Rows {
		if i > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString("(")

		for j, val := range row.Values {
			if j > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(fmt.Sprintf(":%d", placeholderIdx))
			placeholderIdx++
			allValues = append(allValues, val)
		}
		sql.WriteString(")")
	}

	// 执行插入
	result, err := w.client.Exec(sql.String(), allValues...)
	if err != nil {
		// 只输出第一行INSERT语句
		//fullSQL := w.formatInsertSQL(tableName, batch.Columns, []types.DataRow{batch.Rows[0]})
		//return 0, fmt.Errorf("批量插入数据失败: %w\n完整INSERT语句:\n%s", err, fullSQL)
		return 0, fmt.Errorf("批量插入数据失败: %w", err)
	}

	return result.RowsAffected()
}

// TruncateTable 清空表
func (w *DataWriter) TruncateTable(tableName string) error {
	sql := fmt.Sprintf("DELETE FROM %s", w.quoteIdentifier(tableName))
	_, err := w.client.Exec(sql)
	return err
}

// GetRowCount 获取目标表行数
func (w *DataWriter) GetRowCount(tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", w.quoteIdentifier(tableName))

	var count int64
	err := w.client.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("获取行数失败: %w", err)
	}

	return count, nil
}

// quoteIdentifier 引用标识符（转为小写）
func (w *DataWriter) quoteIdentifier(name string) string {
	return fmt.Sprintf(`"%s"`, strings.ToLower(name))
}

// formatValue 将值格式化为SQL字符串
func (w *DataWriter) formatValue(val interface{}) string {
	if val == nil {
		return "NULL"
	}
	switch v := val.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
	case []byte:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(string(v), "'", "''"))
	case time.Time:
		return fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05"))
	case bool:
		if v {
			return "1"
		}
		return "0"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%v", v)
	case float32, float64:
		return fmt.Sprintf("%v", v)
	default:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(fmt.Sprintf("%v", v), "'", "''"))
	}
}

// formatInsertSQL 格式化完整的INSERT语句（包含实际值）
func (w *DataWriter) formatInsertSQL(tableName string, columns []string, rows []types.DataRow) string {
	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = w.quoteIdentifier(col)
	}

	var sb strings.Builder
	for _, row := range rows {
		sb.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES (",
			w.quoteIdentifier(tableName),
			strings.Join(quotedCols, ", ")))

		for j, val := range row.Values {
			if j > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(w.formatValue(val))
		}
		sb.WriteString(");\n")
	}

	return sb.String()
}

// SetClient 更新 DataWriter 的数据库连接
// 用于连接断开后重建连接
func (w *DataWriter) SetClient(client *Client) {
	w.client = client
}

// InsertBatchWithRetry 带重试的批量插入
// 用于处理 Oscar ODBC 驱动在高并发时的连接不稳定问题
// reconnectFunc: 连接重建函数，返回新的 Client 以便更新 DataWriter
func (w *DataWriter) InsertBatchWithRetry(tableName string, batch *types.DataBatch, maxRetries int, reconnectFunc func() (*Client, error)) (int64, error) {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		inserted, err := w.InsertBatchOptimized(tableName, batch)
		if err == nil {
			return inserted, nil
		}
		lastErr = err
		// 检查是否是连接错误，需要重试
		errMsg := err.Error()
		if strings.Contains(errMsg, "bad connection") ||
			strings.Contains(errMsg, "driver") ||
			strings.Contains(errMsg, "connection") {
			// 尝试重建连接并更新 DataWriter
			if reconnectFunc != nil {
				newClient, reconnectErr := reconnectFunc()
				if reconnectErr != nil {
					fmt.Printf("[重试] 重建连接失败: %v\n", reconnectErr)
				} else {
					w.SetClient(newClient) // 关键：更新当前 DataWriter 的连接
				}
			}
			// 指数退避
			sleepTime := time.Duration(i+1) * time.Second
			if sleepTime > 5*time.Second {
				sleepTime = 5 * time.Second
			}
			time.Sleep(sleepTime)
			continue
		}
		// 其他错误直接返回
		return 0, err
	}
	return 0, lastErr
}
