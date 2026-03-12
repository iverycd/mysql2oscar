package oscar

import (
	"fmt"
	"strings"

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
			return inserted, fmt.Errorf("插入数据失败: %w", err)
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
