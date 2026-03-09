package oscar

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "go-aci"
)

// Client Oscar 数据库客户端
type Client struct {
	db *sql.DB
}

// NewClient 创建 Oscar 客户端
func NewClient(host, username, password, database string, port int) (*Client, error) {
	// 构建连接字符串
	connStr := fmt.Sprintf("%s/%s@%s:%d/%s", username, password, host, port, database)

	db, err := sql.Open("aci", connStr)
	if err != nil {
		return nil, fmt.Errorf("连接 Oscar 失败: %w", err)
	}

	// 测试连接
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("Oscar 连接测试失败: %w", err)
	}

	// 设置连接池参数
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute) // 连接最大生命周期
	db.SetConnMaxIdleTime(5 * time.Minute)  // 空闲连接最大存活时间

	return &Client{
		db: db,
	}, nil
}

// Close 关闭连接
func (c *Client) Close() error {
	return c.db.Close()
}

// GetDB 获取数据库连接
func (c *Client) GetDB() *sql.DB {
	return c.db
}

// Exec 执行 SQL
func (c *Client) Exec(query string, args ...interface{}) (sql.Result, error) {
	// 使用带超时的 context
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return c.db.ExecContext(ctx, query, args...)
}

// Query 查询
func (c *Client) Query(query string, args ...interface{}) (*sql.Rows, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return c.db.QueryContext(ctx, query, args...)
}

// QueryRow 查询单行
func (c *Client) QueryRow(query string, args ...interface{}) *sql.Row {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return c.db.QueryRowContext(ctx, query, args...)
}

// Begin 开始事务
func (c *Client) Begin() (*sql.Tx, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.db.BeginTx(ctx, nil)
}

// TableExists 检查表是否存在
func (c *Client) TableExists(tableName string) (bool, error) {
	query := `
		SELECT COUNT(*)
		FROM USER_TABLES
		WHERE TABLE_NAME = :1
	`
	var count int
	err := c.QueryRow(query, tableName).Scan(&count)
	if err != nil {
		// 如果系统表查询失败，尝试其他方式
		query = fmt.Sprintf("SELECT 1 FROM %s WHERE 1=0", c.quoteIdentifier(tableName))
		_, err = c.Exec(query)
		if err != nil {
			return false, nil
		}
		return true, nil
	}
	return count > 0, nil
}

// DropTable 删除表
func (c *Client) DropTable(tableName string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", c.quoteIdentifier(tableName))
	_, err := c.Exec(query)
	return err
}

// DropView 删除视图
func (c *Client) DropView(viewName string) error {
	query := fmt.Sprintf("DROP VIEW IF EXISTS %s", c.quoteIdentifier(viewName))
	_, err := c.Exec(query)
	return err
}

// quoteIdentifier 引用标识符
func (c *Client) quoteIdentifier(name string) string {
	// Oscar 使用双引号引用标识符
	return fmt.Sprintf(`"%s"`, name)
}
