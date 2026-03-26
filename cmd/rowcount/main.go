package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"mysql2oscar/internal/config"
	"mysql2oscar/internal/source/mysql"
	"mysql2oscar/internal/target/oscar"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// TableCountResult 单个表的行数比对结果
type TableCountResult struct {
	TableName   string
	SourceCount int64
	TargetCount int64
	Difference  int64
	Match       bool
	Error       string
}

// CountReport 行数比对报告
type CountReport struct {
	TotalTables    int
	MatchedTables  int
	MismatchTables int
	FailedTables   int
	Results        []TableCountResult
	StartTime      time.Time
	EndTime        time.Time
	SourceDSN      string // 源数据库连接串
	TargetDSN      string // 目标数据库连接串
}

func main() {
	// 解析命令行参数
	configFile := flag.String("config", "config.yaml", "配置文件路径")
	outputFile := flag.String("output", "", "输出CSV文件路径(可选)")
	showVersion := flag.Bool("version", false, "显示版本信息")
	flag.Parse()

	if *showVersion {
		fmt.Printf("rowcount %s (commit: %s, built: %s)\n", version, commit, date)
		os.Exit(0)
	}

	// 加载配置
	cfg, err := config.Load(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "错误: 加载配置文件失败: %v\n", err)
		os.Exit(1)
	}

	// 执行比对
	report, err := compareTableCounts(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "错误: %v\n", err)
		os.Exit(1)
	}

	// 输出报告
	printReport(report)

	// 如果指定了输出文件，导出CSV
	if *outputFile != "" {
		if err := exportCSV(report, *outputFile); err != nil {
			fmt.Fprintf(os.Stderr, "错误: 导出CSV失败: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("\n报告已导出至: %s\n", *outputFile)
	}

	// 如果有不匹配的表，返回非零退出码
	if report.MismatchTables > 0 || report.FailedTables > 0 {
		os.Exit(1)
	}
}

// compareTableCounts 执行表行数比对
func compareTableCounts(cfg *config.Config) (*CountReport, error) {
	report := &CountReport{
		StartTime: time.Now(),
		SourceDSN: fmt.Sprintf("%s@tcp(%s:%d)/%s", cfg.Source.User, cfg.Source.Host, cfg.Source.Port, cfg.Source.Database),
		TargetDSN: fmt.Sprintf("%s@%s:%d/%s", cfg.Target.Username, cfg.Target.Host, cfg.Target.Port, cfg.Target.Database),
	}

	// 创建 MySQL 客户端
	mysqlClient, err := mysql.NewClient(
		cfg.Source.Host,
		cfg.Source.Port,
		cfg.Source.User,
		cfg.Source.Password,
		cfg.Source.Database,
		cfg.Source.Charset,
		2, // 只需要少量连接
	)
	if err != nil {
		return nil, fmt.Errorf("连接 MySQL 失败: %w", err)
	}
	defer mysqlClient.Close()

	// 创建 Oscar 客户端
	oscarClient, err := oscar.NewClient(
		cfg.Target.Host,
		cfg.Target.Username,
		cfg.Target.Password,
		cfg.Target.Database,
		cfg.Target.Port,
		2, // 只需要少量连接
		cfg.Migration.UseUppercase,
	)
	if err != nil {
		return nil, fmt.Errorf("连接 Oscar 失败: %w", err)
	}
	defer oscarClient.Close()

	// 获取表列表
	tables, err := getTableList(cfg, mysqlClient)
	if err != nil {
		return nil, fmt.Errorf("获取表列表失败: %w", err)
	}

	report.TotalTables = len(tables)
	report.Results = make([]TableCountResult, 0, len(tables))

	// 获取数据库连接
	mysqlDB := mysqlClient.GetDB()
	oscarDB := oscarClient.GetDB()

	// 逐表比对
	for _, tableName := range tables {
		result := compareSingleTable(tableName, mysqlDB, oscarDB, cfg.Migration.UseUppercase)
		report.Results = append(report.Results, result)

		if result.Error != "" {
			report.FailedTables++
		} else if result.Match {
			report.MatchedTables++
		} else {
			report.MismatchTables++
		}
	}

	report.EndTime = time.Now()
	return report, nil
}

// getTableList 获取要比对的表列表
func getTableList(cfg *config.Config, mysqlClient *mysql.Client) ([]string, error) {
	// 如果配置中指定了表列表，使用配置的表
	if len(cfg.Migration.Tables) > 0 {
		return cfg.Migration.Tables, nil
	}

	// 否则获取所有表
	schemaReader := mysql.NewSchemaReader(mysqlClient)
	tables, err := schemaReader.GetTables()
	if err != nil {
		return nil, fmt.Errorf("查询表列表失败: %w", err)
	}
	return tables, nil
}

// compareSingleTable 比对单个表的行数
func compareSingleTable(tableName string, mysqlDB *sql.DB, oscarDB *sql.DB, useUppercase bool) TableCountResult {
	result := TableCountResult{
		TableName: tableName,
	}

	// 获取 MySQL 行数
	sourceCount, err := getMySQLRowCount(mysqlDB, tableName)
	if err != nil {
		result.Error = fmt.Sprintf("MySQL: %v", err)
		return result
	}
	result.SourceCount = sourceCount

	// 获取 Oscar 行数（根据配置转换大小写）
	var oscarTableName string
	if useUppercase {
		oscarTableName = strings.ToUpper(tableName)
	} else {
		oscarTableName = strings.ToLower(tableName)
	}
	targetCount, err := getOscarRowCount(oscarDB, oscarTableName)
	if err != nil {
		result.Error = fmt.Sprintf("Oscar: %v", err)
		return result
	}
	result.TargetCount = targetCount

	// 计算差异
	result.Difference = result.TargetCount - result.SourceCount
	result.Match = result.Difference == 0

	return result
}

// getMySQLRowCount 获取 MySQL 表行数
func getMySQLRowCount(db *sql.DB, tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)
	var count int64
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("获取行数失败: %w", err)
	}
	return count, nil
}

// getOscarRowCount 获取 Oscar 表行数
func getOscarRowCount(db *sql.DB, tableName string) (int64, error) {
	query := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, tableName)
	var count float64
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("获取行数失败: %w", err)
	}
	return int64(count), nil
}

// printReport 打印比对报告
func printReport(report *CountReport) {
	fmt.Println("\n表行数比对报告")
	fmt.Println("================")
	fmt.Printf("开始时间: %s\n", report.StartTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("结束时间: %s\n", report.EndTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("耗时: %v\n\n", report.EndTime.Sub(report.StartTime).Round(time.Millisecond))

	// 创建表格写入器
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	// 表头
	fmt.Fprintln(w, "表名\t源行数\t目标行数\t差异\t状态")
	fmt.Fprintln(w, strings.Repeat("-", 80))

	// 表内容
	for _, r := range report.Results {
		var status string
		if r.Error != "" {
			status = fmt.Sprintf("错误: %s", truncateError(r.Error, 30))
		} else if r.Match {
			status = "✓"
		} else {
			status = "✗"
		}

		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%s\n",
			r.TableName,
			r.SourceCount,
			r.TargetCount,
			r.Difference,
			status)
	}
	w.Flush()

	// 统计信息
	fmt.Println()
	fmt.Printf("统计: 总表数 %d, 匹配 %d, 不匹配 %d, 失败 %d\n",
		report.TotalTables,
		report.MatchedTables,
		report.MismatchTables,
		report.FailedTables,
	)
	fmt.Printf("源库: %s\n", report.SourceDSN)
	fmt.Printf("目标: %s\n", report.TargetDSN)

	// 如果有不匹配的表，列出详情
	if report.MismatchTables > 0 {
		fmt.Println("\n不匹配的表:")
		for _, r := range report.Results {
			if !r.Match && r.Error == "" {
				fmt.Printf("  - %s: 源=%d, 目标=%d, 差异=%d\n",
					r.TableName, r.SourceCount, r.TargetCount, r.Difference)
			}
		}
	}

	// 如果有失败的表，列出详情
	if report.FailedTables > 0 {
		fmt.Println("\n失败的表:")
		for _, r := range report.Results {
			if r.Error != "" {
				fmt.Printf("  - %s: %s\n", r.TableName, r.Error)
			}
		}
	}
}

// exportCSV 导出报告为CSV文件
func exportCSV(report *CountReport, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入表头
	if err := writer.Write([]string{"表名", "源行数", "目标行数", "差异", "状态", "错误信息"}); err != nil {
		return err
	}

	// 写入数据
	for _, r := range report.Results {
		var status string
		if r.Error != "" {
			status = "错误"
		} else if r.Match {
			status = "匹配"
		} else {
			status = "不匹配"
		}

		if err := writer.Write([]string{
			r.TableName,
			fmt.Sprintf("%d", r.SourceCount),
			fmt.Sprintf("%d", r.TargetCount),
			fmt.Sprintf("%d", r.Difference),
			status,
			r.Error,
		}); err != nil {
			return err
		}
	}

	return nil
}

// truncateError 截断错误信息
func truncateError(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
