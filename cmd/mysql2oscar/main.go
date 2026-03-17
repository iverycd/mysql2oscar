package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"mysql2oscar/internal/config"
	"mysql2oscar/internal/migrator"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func init() {
	// 设置日志格式：显示日期、时间、文件名和行号
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	// 解析命令行参数
	configFile := flag.String("config", "config.yaml", "配置文件路径")
	showVersion := flag.Bool("version", false, "显示版本信息")
	flag.Parse()

	if *showVersion {
		fmt.Printf("mysql2oscar %s (commit: %s, built: %s)\n", version, commit, date)
		os.Exit(0)
	}

	// 加载配置
	cfg, err := config.Load(*configFile)
	if err != nil {
		log.Fatalf("加载配置文件失败: %v", err)
	}

	log.Printf("开始迁移: MySQL %s:%d/%s ->  Oscar %s:%d/%s 用户:%s",
		cfg.Source.Host, cfg.Source.Port, cfg.Source.Database, cfg.Target.Host, cfg.Target.Port, cfg.Target.Database, cfg.Target.Username)

	// 创建迁移器
	m, err := migrator.New(cfg)
	if err != nil {
		log.Fatalf("创建迁移器失败: %v", err)
	}

	// 设置源数据库名（用于视图转换时移除数据库前缀）
	m.SetSourceDB(cfg.Source.Database)

	// 执行迁移
	result, err := m.Migrate()
	if err != nil {
		m.Close() // 确保错误情况下也关闭连接
		log.Fatalf("迁移失败: %v", err)
	}

	// 先关闭连接和日志文件
	m.Close()

	// 最后输出结果
	fmt.Println("\n迁移完成!")
	fmt.Printf("  表迁移: 成功 %d, 失败 %d\n", result.TablesMigrated, result.TablesFailed)
	fmt.Printf("  视图迁移: 成功 %d, 失败 %d\n", result.ViewsMigrated, result.ViewsFailed)
	fmt.Printf("  总行数: %d\n", result.TotalRows)
	fmt.Printf("  总耗时: %v\n", result.TotalTime)

	if len(result.FailedTables) > 0 {
		fmt.Println("\n失败的表:")
		for _, t := range result.FailedTables {
			fmt.Printf("  - %s\n", t)
		}
	}

	if len(result.FailedViews) > 0 {
		fmt.Println("\n失败的视图:")
		for _, v := range result.FailedViews {
			fmt.Printf("  - %s\n", v)
		}
	}
}
