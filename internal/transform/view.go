package transform

import (
	"fmt"
	"regexp"
	"strings"
)

// ViewConverter 视图 SQL 转换器
type ViewConverter struct {
	// MySQL 到 Oscar 的函数映射
	functionMappings map[string]string
	// 源数据库名
	sourceDB string
}

// NewViewConverter 创建视图转换器
func NewViewConverter() *ViewConverter {
	return &ViewConverter{
		functionMappings: map[string]string{
			"NOW":          "CURRENT_TIMESTAMP",
			"CURDATE":      "CURRENT_DATE",
			"CURTIME":      "CURRENT_TIME",
			"IFNULL":       "COALESCE",
			"DATE_FORMAT":  "TO_CHAR",
			"STR_TO_DATE":  "TO_DATE",
			"GROUP_CONCAT": "LISTAGG",
		},
	}
}

// SetSourceDB 设置源数据库名
func (c *ViewConverter) SetSourceDB(sourceDB string) {
	c.sourceDB = sourceDB
}

// ConvertViewSQL 转换视图 SQL
func (c *ViewConverter) ConvertViewSQL(mysqlSQL string) string {
	sql := mysqlSQL

	// 1. 移除源数据库名引用
	// 例如: `a1`.`table_name` -> `table_name`, a1.table_name -> table_name
	if c.sourceDB != "" {
		// 匹配 `db_name`.`table_name` 格式
		dbBacktickPattern := fmt.Sprintf("`%s`\\.", c.sourceDB)
		sql = regexp.MustCompile(dbBacktickPattern).ReplaceAllString(sql, "")

		// 匹配 db_name.table_name 格式（不带引号）
		dbPlainPattern := fmt.Sprintf("\\b%s\\.", c.sourceDB)
		sql = regexp.MustCompile(dbPlainPattern).ReplaceAllString(sql, "")

		// 匹配 "db_name"."table_name" 格式
		dbDoubleQuotePattern := fmt.Sprintf("\"%s\"\\.", c.sourceDB)
		sql = regexp.MustCompile(dbDoubleQuotePattern).ReplaceAllString(sql, "")
	}

	// 2. 转换函数名
	for mysqlFunc, oscarFunc := range c.functionMappings {
		// 匹配函数名（不区分大小写）
		re := regexp.MustCompile(fmt.Sprintf(`(?i)\b%s\b`, mysqlFunc))
		sql = re.ReplaceAllString(sql, oscarFunc)
	}

	// 3. 处理反引号引用 -> 双引号
	sql = strings.ReplaceAll(sql, "`", "\"")

	return sql
}

// ConvertWhereClause 转换 WHERE 子句
func (c *ViewConverter) ConvertWhereClause(where string) string {
	// 处理 MySQL 特有的 WHERE 语法
	sql := where

	// 转换日期比较
	// DATE(column) = '2023-01-01' -> column::date = '2023-01-01'
	dateFunc := regexp.MustCompile(`(?i)DATE\(([^)]+)\)`)
	sql = dateFunc.ReplaceAllString(sql, "$1::date")

	return sql
}

// ConvertJoinClause 转换 JOIN 子句
func (c *ViewConverter) ConvertJoinClause(join string) string {
	// MySQL 的 JOIN 语法通常与标准 SQL 兼容
	// 这里处理一些特殊情况
	return join
}