package migrator

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"mysql2oscar/internal/config"
	"mysql2oscar/internal/source/mysql"
	"mysql2oscar/internal/target/oscar"
	"mysql2oscar/internal/transform"
	"mysql2oscar/pkg/types"
)

// Migrator 迁移器
type Migrator struct {
	cfg           *config.Config
	mysqlClient   *mysql.Client // MySQL 连接保持不变（读操作稳定）
	schemaReader  *mysql.SchemaReader
	dataReader    *mysql.DataReader
	typeMapper    *transform.TypeMapper
	viewConverter *transform.ViewConverter
	logger        *Logger

	// 目标数据库连接信息（用于创建临时连接）
	targetHost     string
	targetPort     int
	targetUsername string
	targetPassword string
	targetDatabase string

	// 迁移过程跟踪
	failedTableCreate sync.Map       // 表结构创建失败的表（线程安全）
	autoIncrColumns   []autoIncrInfo // 需要设置自增的列信息
	autoIncrMutex     sync.Mutex     // 保护 autoIncrColumns
}

// autoIncrInfo 自增列信息
type autoIncrInfo struct {
	tableName     string
	columnName    string
	autoIncrement int64 // 自增起始值
}

// New 创建迁移器
func New(cfg *config.Config) (*Migrator, error) {
	// 创建日志管理器
	logger, err := NewLogger()
	if err != nil {
		return nil, fmt.Errorf("创建日志管理器失败: %w", err)
	}

	// 计算连接池大小
	maxConns := cfg.Migration.Parallelism + 5 // 缓冲

	log.Printf("[连接池] MySQL 最大连接数: %d (parallelism=%d)", maxConns, cfg.Migration.Parallelism)

	// 创建 MySQL 客户端
	mysqlClient, err := mysql.NewClient(
		cfg.Source.Host,
		cfg.Source.Port,
		cfg.Source.User,
		cfg.Source.Password,
		cfg.Source.Database,
		cfg.Source.Charset,
		maxConns,
	)
	if err != nil {
		logger.Close()
		return nil, fmt.Errorf("创建 MySQL 客户端失败: %w", err)
	}

	// 不再创建全局 Oscar 客户端，改为表级别创建临时连接
	// 测试 Oscar 连接是否可用
	testClient, err := oscar.NewTempClient(
		cfg.Target.Host,
		cfg.Target.Username,
		cfg.Target.Password,
		cfg.Target.Database,
		cfg.Target.Port,
	)
	if err != nil {
		mysqlClient.Close()
		logger.Close()
		return nil, fmt.Errorf("测试 Oscar 连接失败: %w", err)
	}
	testClient.Close()
	log.Printf("[连接池] Oscar 使用表级别连接（每个表独立连接）")

	return &Migrator{
		cfg:             cfg,
		mysqlClient:     mysqlClient,
		schemaReader:    mysql.NewSchemaReader(mysqlClient),
		dataReader:      mysql.NewDataReader(mysqlClient, cfg.Migration.BatchSize),
		typeMapper:      transform.NewTypeMapper(),
		viewConverter:   transform.NewViewConverter(),
		logger:          logger,
		autoIncrColumns: make([]autoIncrInfo, 0),
		// 目标数据库连接信息
		targetHost:     cfg.Target.Host,
		targetPort:     cfg.Target.Port,
		targetUsername: cfg.Target.Username,
		targetPassword: cfg.Target.Password,
		targetDatabase: cfg.Target.Database,
	}, nil
}

// SetSourceDB 设置源数据库��（用于视图转换）
func (m *Migrator) SetSourceDB(sourceDB string) {
	m.viewConverter.SetSourceDB(sourceDB)
}

// createTempClient 创建临时 Oscar 连接
func (m *Migrator) createTempClient() (*oscar.Client, error) {
	return oscar.NewTempClient(
		m.targetHost,
		m.targetUsername,
		m.targetPassword,
		m.targetDatabase,
		m.targetPort,
	)
}

// tableResult 单表迁移结果
type tableResult struct {
	tableName string
	rowCount  int64
	err       error
	errMsg    string
	errSQL    string
	elapsed   time.Duration
}

// migrateSingleTable 迁移单个表（使用独立连接）
// 包含完整的四阶段迁移：创建表结构���迁移数据、创建索引和外键、设置自增列
func (m *Migrator) migrateSingleTable(tableName string, autoIncrInfoMap map[string]mysql.AutoIncrementInfo) *tableResult {
	result := &tableResult{tableName: tableName}

	// 1. 读取表结构
	table, err := m.schemaReader.GetTableSchema(tableName)
	if err != nil {
		result.err = err
		result.errMsg = fmt.Sprintf("读取表结构失败: %v", err)
		return result
	}

	// 2. 创建新的 Oscar 连接
	client, err := m.createTempClient()
	if err != nil {
		result.err = err
		result.errMsg = fmt.Sprintf("创建 Oscar 连接失败: %v", err)
		return result
	}
	defer client.Close() // 确保迁移完成后关闭连接

	// 3. 创建 SchemaWriter 和 DataWriter
	schemaWriter := oscar.NewSchemaWriter(client)
	dataWriter := oscar.NewDataWriter(client)

	// 4. 处理已存在的表
	if m.cfg.Migration.Overwrite {
		if err := client.DropTable(tableName); err != nil {
			result.err = err
			result.errMsg = fmt.Sprintf("删除表失败: %v", err)
			result.errSQL = fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tableName)
			return result
		}
	} else {
		exists, err := client.TableExists(tableName)
		if err != nil {
			result.err = err
			result.errMsg = fmt.Sprintf("检查表是否存在失败: %v", err)
			return result
		}
		if exists {
			result.err = fmt.Errorf("表已存在")
			result.errMsg = "表已存在"
			return result
		}
	}

	// 5. 创建表结构（不包含自增属性）
	sql, err := schemaWriter.CreateTableWithoutAutoIncr(table)
	if err != nil {
		result.err = err
		result.errMsg = fmt.Sprintf("%v", err)
		result.errSQL = sql
		return result
	}

	// 6. 添加表注释
	if table.Comment != "" {
		_, err := schemaWriter.AddTableComment(tableName, table.Comment)
		if err != nil {
			log.Printf("[警告] 表 %s: 添加表注释失败: %v", tableName, err)
		}
	}

	// 7. 添加列注释
	failedColComments := schemaWriter.AddColumnComments(tableName, table.Columns)
	if len(failedColComments) > 0 {
		log.Printf("[警告] 表 %s: %d 个列注释添加失败", tableName, len(failedColComments))
	}

	// 8. 迁移数据
	rowCount, err := m.migrateTableDataWithWriter(tableName, table.Columns, dataWriter)
	if err != nil {
		result.err = err
		result.errMsg = fmt.Sprintf("迁移数据失败: %v", err)
		return result
	}
	result.rowCount = rowCount

	// 9. 创建主键
	for _, idx := range table.Indexes {
		if idx.IsPrimary {
			sql, err := schemaWriter.AddPrimaryKey(tableName, idx.Columns)
			if err != nil {
				m.logger.LogIndexCreateFailed(tableName, "PRIMARY", sql, err.Error())
				log.Printf("[警告] 表 %s: 添加主键失败: %v", tableName, err)
			} else {
				log.Printf("[完成] 表 %s: 添加主键 (%s)", tableName, idx.Name)
			}
			break // 每个表只有一个主键
		}
	}

	// 10. 创建索引
	if m.cfg.Migration.MigrateIndexes && len(table.Indexes) > 0 {
		failedIndexes := schemaWriter.CreateIndexes(tableName, table.Indexes)
		for _, fi := range failedIndexes {
			m.logger.LogIndexCreateFailed(tableName, fi.IndexName, fi.SQL, fi.Err.Error())
		}
		if len(failedIndexes) == 0 {
			log.Printf("[完成] 表 %s: 创建了 %d 个索引", tableName, len(table.Indexes))
		} else {
			log.Printf("[部分完成] 表 %s: 成功 %d 个索引, 失败 %d 个索引", tableName, len(table.Indexes)-len(failedIndexes), len(failedIndexes))
		}
	}

	// 11. 创建外键
	if len(table.ForeignKeys) > 0 {
		for _, fk := range table.ForeignKeys {
			sql, err := schemaWriter.CreateSingleForeignKey(tableName, fk)
			if err != nil {
				m.logger.LogFkCreateFailed(tableName, fk.Name, sql, err.Error())
			}
		}
		log.Printf("[完成] 表 %s: 处理了 %d 个外键", tableName, len(table.ForeignKeys))
	}

	// 12. 设置自增列（如果有）
	for _, col := range table.Columns {
		if col.IsAutoIncr {
			startValue := int64(1)
			if info, ok := autoIncrInfoMap[tableName]; ok {
				startValue = info.AutoIncrement
			}

			// 先删除可能存在的序列
			schemaWriter.DropSequence(tableName, col.Name)

			// 创建序列
			sql, err := schemaWriter.CreateSequence(tableName, col.Name, startValue)
			if err != nil {
				m.logger.LogAutoIncrFailed(tableName, col.Name, sql, fmt.Sprintf("创建序列失败: %v", err))
				log.Printf("[警告] 表 %s 自增列 %s: 创建序列失败: %v", tableName, col.Name, err)
				continue
			}

			// 设置列的默认值为序列的下一个值
			sql, err = schemaWriter.SetColumnDefaultSequence(tableName, col.Name)
			if err != nil {
				m.logger.LogAutoIncrFailed(tableName, col.Name, sql, fmt.Sprintf("设置列默认值为序列失败: %v", err))
				log.Printf("[警告] 表 %s 自增列 %s: 设置默认值失败: %v", tableName, col.Name, err)
				continue
			}

			log.Printf("[完成] 表 %s 自增列 %s 设置成功 (起始值: %d)", tableName, col.Name, startValue)
		}
	}

	return result
}

// migrateTableDataWithWriter 使用指定的 DataWriter 迁移表数据
func (m *Migrator) migrateTableDataWithWriter(tableName string, columns []types.Column, dataWriter *oscar.DataWriter) (int64, error) {
	var totalRows int64

	// 获取列名
	colNames := make([]string, len(columns))
	for i, col := range columns {
		colNames[i] = col.Name
	}

	// 流式读取并批量写入
	err := m.dataReader.ReadTableData(tableName, colNames, func(batch *types.DataBatch) error {
		// 使用带重试的批量插入，最多重试3次
		inserted, err := dataWriter.InsertBatchWithRetry(tableName, batch, 3)
		if err != nil {
			return err
		}
		totalRows += inserted

		// 进度日志
		if totalRows%10000 == 0 {
			log.Printf("[进度] 表 %s: 已迁移 %d 行", tableName, totalRows)
		}

		return nil
	})

	return totalRows, err
}

// Close 关闭资源
func (m *Migrator) Close() error {
	var errs []error

	if m.mysqlClient != nil {
		if err := m.mysqlClient.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// Oscar 连接现在是表级别的，在迁移完成后自动关闭

	if m.logger != nil {
		if err := m.logger.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("关闭连接时发生错误: %v", errs)
	}

	return nil
}

// Migrate 执行迁移
func (m *Migrator) Migrate() (*types.MigrationResult, error) {
	startTime := time.Now()
	result := &types.MigrationResult{}

	// 获取要迁移的表列表
	tables, err := m.getTablesToMigrate()
	if err != nil {
		return nil, err
	}

	log.Printf("发现 %d 个表需要迁移", len(tables))

	// 迁移表结构数据
	result = m.migrateTables(tables)

	// 迁移视图
	if m.cfg.Migration.MigrateViews {
		viewResult := m.migrateViews()
		result.ViewsMigrated = viewResult.ViewsMigrated
		result.ViewsFailed = viewResult.ViewsFailed
		result.FailedViews = viewResult.FailedViews
	}

	result.TotalTime = time.Since(startTime)
	return result, nil
}

// getTablesToMigrate 获取要迁移的表列表
func (m *Migrator) getTablesToMigrate() ([]string, error) {
	if len(m.cfg.Migration.Tables) > 0 {
		return m.cfg.Migration.Tables, nil
	}

	// 获取所有表
	return m.schemaReader.GetTables()
}

// schemaResult 表结构创建结果（保留用于日志兼容）
type schemaResult struct {
	tableName string
	table     *types.Table
	err       error
	errMsg    string
	errSQL    string
	elapsed   time.Duration
}

// migrateTables 迁移表（表级别连接管理）
// 每个表使用独立的连接，包含完整的四阶段迁移
func (m *Migrator) migrateTables(tables []string) *types.MigrationResult {
	result := &types.MigrationResult{}

	// 预先获取所有表的自增列信息（包含起始值）
	autoIncrInfoMap, err := m.schemaReader.GetAutoIncrementInfo()
	if err != nil {
		log.Printf("[注意] 获取自增列信息失败: %v，将使用默认起始值1", err)
		autoIncrInfoMap = make(map[string]mysql.AutoIncrementInfo)
	}

	log.Printf("========== 开始迁移表（并发数: %d，每个表使用独立连接）==========", m.cfg.Migration.Parallelism)

	// 使用 worker pool 并行迁移表
	jobs := make(chan string, len(tables))
	results := make(chan *tableResult, len(tables))

	var wg sync.WaitGroup
	var completedCount int64 // 原子计数器

	// 启动 worker
	for i := 0; i < m.cfg.Migration.Parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tableName := range jobs {
				startTime := time.Now()
				r := m.migrateSingleTable(tableName, autoIncrInfoMap)
				r.elapsed = time.Since(startTime)

				// 更新进度计数
				current := atomic.AddInt64(&completedCount, 1)
				if r.err != nil {
					log.Printf("[%d/%d] 表 %s: 失败 - %s (%v)", current, len(tables), tableName, r.errMsg, r.elapsed)
				} else {
					log.Printf("[%d/%d] 表 %s: 成功 - %d 行 (%v)", current, len(tables), tableName, r.rowCount, r.elapsed)
				}

				results <- r
			}
		}()
	}

	// 发送任务
	for _, tableName := range tables {
		jobs <- tableName
	}
	close(jobs)

	// 等待完成
	go func() {
		wg.Wait()
		close(results)
	}()

	// 收集结果
	for r := range results {
		if r.err != nil {
			result.TablesFailed++
			result.FailedTables = append(result.FailedTables, r.tableName)
			m.failedTableCreate.Store(r.tableName, true)
			if r.errSQL != "" {
				m.logger.LogTableCreateFailed(r.tableName, r.errSQL, r.errMsg)
			} else {
				m.logger.LogTableDataError(r.tableName, "", r.errMsg)
			}
		} else {
			result.TablesMigrated++
			result.TotalRows += r.rowCount
		}
	}

	return result
}

// migrateViews 迁移视图（使用独立连接）
func (m *Migrator) migrateViews() *types.MigrationResult {
	result := &types.MigrationResult{}

	views, err := m.schemaReader.GetViews()
	if err != nil {
		log.Printf("获取视图列表失败: %v", err)
		return result
	}

	log.Printf("发现 %d 个视图需要迁移", len(views))

	// 创建临时连接用于视图迁移
	client, err := m.createTempClient()
	if err != nil {
		log.Printf("创建 Oscar 连接失败: %v", err)
		return result
	}
	defer client.Close()

	schemaWriter := oscar.NewSchemaWriter(client)

	for _, viewName := range views {
		view, err := m.schemaReader.GetViewDefinition(viewName)
		if err != nil {
			result.ViewsFailed++
			result.FailedViews = append(result.FailedViews, viewName)
			m.logger.LogViewCreateFailed(viewName, "", fmt.Sprintf("获取视图定义失败: %v", err))
			continue
		}

		// 转换视图 SQL
		view.Definition = m.viewConverter.ConvertViewSQL(view.Definition)

		// 检查视图是否存在
		exists, _ := client.TableExists(viewName)
		if exists {
			client.DropView(viewName)
		}

		// 创建视图
		sql, err := schemaWriter.CreateView(view)
		if err != nil {
			result.ViewsFailed++
			result.FailedViews = append(result.FailedViews, viewName)
			m.logger.LogViewCreateFailed(viewName, sql, err.Error())
			continue
		}

		result.ViewsMigrated++
		log.Printf("[完成] 视图 %s", viewName)
	}

	return result
}
