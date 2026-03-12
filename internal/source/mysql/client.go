package mysql

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

// Client MySQL 客户端
type Client struct {
	db     *sql.DB
	dbName string
}

// NewClient 创建 MySQL 客户端
// maxConns: 最大连接数，应设置为 parallelism + 缓冲
func NewClient(host string, port int, user, password, database, charset string, maxConns int) (*Client, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=true&loc=Local",
		user, password, host, port, database, charset)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接 MySQL 失败: %w", err)
	}

	// 测试连接
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("MySQL 连接测试失败: %w", err)
	}

	// 设置连接池参数（根据 parallelism 动态调整）
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns) // 与 MaxOpenConns 相同，避免频繁创建连接
	db.SetConnMaxLifetime(0)     // 不限制连接生命周期

	return &Client{
		db:     db,
		dbName: database,
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

// GetDatabaseName 获取数据库名
func (c *Client) GetDatabaseName() string {
	return c.dbName
}
