package migrator

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Logger 日志管理器
type Logger struct {
	baseDir      string // 日志基础目录 (log/)
	timestampDir string // 时间戳子目录 (log/2026_03_10_15_19_01/)

	// 各类日志文件
	tableCreateFailed *logFile // 表结构创建失败日志
	fkCreateFailed    *logFile // 外键创建失败日志
	idxCreateFailed   *logFile // 索引创建失败日志
	seqCreateFailed   *logFile // 序列创建失败日志
	viewCreateFailed  *logFile // 视图创建失败日志
	autoIncrFailed    *logFile // 自增列创建失败日志
	constraintFailed  *logFile // 约束创建失败日志
	errorTableData    *logFile // 表数据迁移错误日志

	mu sync.Mutex
}

type logFile struct {
	filename string   // 日志文件名
	file     *os.File // 文件句柄（首次写入时创建）
	logger   *log.Logger
	once     sync.Once // 保证只创建一次
}

// NewLogger 创建日志管理器
func NewLogger() (*Logger, error) {
	// 创建 log 目录
	logDir := "log"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("创建日志目录失败: %w", err)
	}

	// 创建时间戳子目录
	timestamp := time.Now().Format("2006_01_02_15_04_05")
	timestampDir := filepath.Join(logDir, timestamp)
	if err := os.MkdirAll(timestampDir, 0755); err != nil {
		return nil, fmt.Errorf("创建时间戳日志目录失败: %w", err)
	}

	l := &Logger{
		baseDir:      logDir,
		timestampDir: timestampDir,
	}

	// 初始化各类日志文件（懒加载，只设置文件名）
	l.tableCreateFailed = &logFile{filename: "tableCreateFailed.log"}
	l.fkCreateFailed = &logFile{filename: "FkCreateFailed.log"}
	l.idxCreateFailed = &logFile{filename: "idxCreateFailed.log"}
	l.seqCreateFailed = &logFile{filename: "seqCreateFailed.log"}
	l.viewCreateFailed = &logFile{filename: "viewCreateFailed.log"}
	l.autoIncrFailed = &logFile{filename: "autoIncrFailed.log"}
	l.constraintFailed = &logFile{filename: "constraintFailed.log"}
	l.errorTableData = &logFile{filename: "errorTableData.log"}

	return l, nil
}

// getOrCreateLogFile 懒加载创建日志文件
func (l *Logger) getOrCreateLogFile(lf *logFile) {
	lf.once.Do(func() {
		filePath := filepath.Join(l.timestampDir, lf.filename)
		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Printf("警告: 创建日志文件 %s 失败: %v", filePath, err)
			return
		}
		lf.file = file
		lf.logger = log.New(file, "", log.LstdFlags)
	})
}

// LogTableCreateFailed 记录表结构创建失败
func (l *Logger) LogTableCreateFailed(tableName, sql, reason string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	msg := fmt.Sprintf("表 %s 创建失败\n%s\n%s", tableName, sql, reason)
	log.Printf("[表结构失败] %s", reason)
	l.writeLog(l.tableCreateFailed, msg)
}

// LogFkCreateFailed 记录外键创建失败
func (l *Logger) LogFkCreateFailed(tableName, fkName, sql, reason string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	msg := fmt.Sprintf("表 %s 外键 %s 创建失败\nSQL: %s\n原因: %s", tableName, fkName, sql, reason)
	log.Printf("[外键失败] 表 %s 外键 %s 创建失败: %s", tableName, fkName, reason)
	l.writeLog(l.fkCreateFailed, msg)
}

// LogIndexCreateFailed 记录索引创建失败
func (l *Logger) LogIndexCreateFailed(tableName, idxName, sql, reason string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	msg := fmt.Sprintf("表 %s 索引 %s 创建失败\nSQL: %s\n原因: %s", tableName, idxName, sql, reason)
	log.Printf("[索引失败] 表 %s 索引 %s 创建失败: %s", tableName, idxName, reason)
	l.writeLog(l.idxCreateFailed, msg)
}

// LogSeqCreateFailed 记录序列创建失败
func (l *Logger) LogSeqCreateFailed(seqName, sql, reason string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	msg := fmt.Sprintf("序列 %s 创建失败\nSQL: %s\n原因: %s", seqName, sql, reason)
	log.Printf("[序列失败] 序列 %s 创建失败: %s", seqName, reason)
	l.writeLog(l.seqCreateFailed, msg)
}

// LogViewCreateFailed 记录视图创建失败
func (l *Logger) LogViewCreateFailed(viewName, sql, reason string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	msg := fmt.Sprintf("视图 %s 创建失败\nSQL: %s\n原因: %s", viewName, sql, reason)
	log.Printf("[视图失败] 视图 %s 创建失败: %s", viewName, reason)
	l.writeLog(l.viewCreateFailed, msg)
}

// LogAutoIncrFailed 记录自增列创建失败
func (l *Logger) LogAutoIncrFailed(tableName, columnName, sql, reason string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	msg := fmt.Sprintf("表 %s 自增列 %s 设置失败\nSQL: %s\n原因: %s", tableName, columnName, sql, reason)
	log.Printf("[自增列失败] 表 %s 自增列 %s 设置失败: %s", tableName, columnName, reason)
	l.writeLog(l.autoIncrFailed, msg)
}

// LogConstraintFailed 记录约束创建失败
func (l *Logger) LogConstraintFailed(tableName, constraintName, sql, reason string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	msg := fmt.Sprintf("表 %s 约束 %s 创建失败\nSQL: %s\n原因: %s", tableName, constraintName, sql, reason)
	log.Printf("[约束失败] 表 %s 约束 %s 创建失败: %s", tableName, constraintName, reason)
	l.writeLog(l.constraintFailed, msg)
}

// LogTableDataError 记录表数据迁移错误
func (l *Logger) LogTableDataError(tableName, sql, reason string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	msg := fmt.Sprintf("表 %s 数据迁移失败\nSQL: %s\n原因: %s", tableName, sql, reason)
	log.Printf("[数据失败] 表 %s 数据迁移失败: %s", tableName, reason)
	l.writeLog(l.errorTableData, msg)
}

// writeLog 写入日志
func (l *Logger) writeLog(lf *logFile, msg string) {
	l.getOrCreateLogFile(lf)
	if lf.logger != nil {
		lf.logger.Println(msg)
	}
}

// GetLogDir 获取日志目录路径
func (l *Logger) GetLogDir() string {
	return l.timestampDir
}

// Close 关闭所有日志文件
func (l *Logger) Close() error {
	var errs []error

	closeFile := func(lf *logFile, name string) {
		if lf.file != nil {
			if err := lf.file.Close(); err != nil {
				errs = append(errs, fmt.Errorf("关闭 %s 失败: %w", name, err))
			}
		}
	}

	closeFile(l.tableCreateFailed, "tableCreateFailed.log")
	closeFile(l.fkCreateFailed, "FkCreateFailed.log")
	closeFile(l.idxCreateFailed, "idxCreateFailed.log")
	closeFile(l.seqCreateFailed, "seqCreateFailed.log")
	closeFile(l.viewCreateFailed, "viewCreateFailed.log")
	closeFile(l.autoIncrFailed, "autoIncrFailed.log")
	closeFile(l.constraintFailed, "constraintFailed.log")
	closeFile(l.errorTableData, "errorTableData.log")

	if len(errs) > 0 {
		return fmt.Errorf("关闭日志文件时发生错误: %v", errs)
	}

	return nil
}

// WriteConsole 同时输出到控制台的 Writer
type consoleWriter struct{}

func (w *consoleWriter) Write(p []byte) (n int, err error) {
	return io.Discard.Write(p) // 我们已经在每个 Log 函数中单独输出到控制台了
}
