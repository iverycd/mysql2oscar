package migrator

import (
	"fmt"
	"log"
	"sync"
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
	mysqlClient   *mysql.Client
	oscarClient   *oscar.Client
	schemaReader  *mysql.SchemaReader
	dataReader    *mysql.DataReader
	schemaWriter  *oscar.SchemaWriter
	dataWriter    *oscar.DataWriter
	typeMapper    *transform.TypeMapper
	viewConverter *transform.ViewConverter
	logger        *Logger // 日志管理器

	// 迁移过程跟踪
	failedTableCreate map[string]bool // 表结构创建失败的表
	autoIncrColumns   []autoIncrInfo  // 需要设置自增的列信息
}

// autoIncrInfo 自增列信息
type autoIncrInfo struct {
	tableName  string
	columnName string
}

// New 创建迁移器
func New(cfg *config.Config) (*Migrator, error) {
	// 创建日志管理器
	logger, err := NewLogger()
	if err != nil {
		return nil, fmt.Errorf("创建日志管理器失败: %w", err)
	}

	// 创建 MySQL 客户端
	mysqlClient, err := mysql.NewClient(
		cfg.Source.Host,
		cfg.Source.Port,
		cfg.Source.User,
		cfg.Source.Password,
		cfg.Source.Database,
		cfg.Source.Charset,
	)
	if err != nil {
		logger.Close()
		return nil, fmt.Errorf("创建 MySQL 客户端失败: %w", err)
	}

	// 创建 Oscar 客户端
	oscarClient, err := oscar.NewClient(
		cfg.Target.Host,
		cfg.Target.Username,
		cfg.Target.Password,
		cfg.Target.Database,
		cfg.Target.Port,
	)
	if err != nil {
		mysqlClient.Close()
		logger.Close()
		return nil, fmt.Errorf("创建 Oscar 客户端失败: %w", err)
	}

	return &Migrator{
		cfg:               cfg,
		mysqlClient:       mysqlClient,
		oscarClient:       oscarClient,
		schemaReader:      mysql.NewSchemaReader(mysqlClient),
		dataReader:        mysql.NewDataReader(mysqlClient, cfg.Migration.BatchSize),
		schemaWriter:      oscar.NewSchemaWriter(oscarClient),
		dataWriter:        oscar.NewDataWriter(oscarClient),
		typeMapper:        transform.NewTypeMapper(),
		viewConverter:     transform.NewViewConverter(),
		logger:            logger,
		failedTableCreate: make(map[string]bool),
		autoIncrColumns:   make([]autoIncrInfo, 0),
	}, nil
}

// SetSourceDB 设置源数据库名（用于视图转换）
func (m *Migrator) SetSourceDB(sourceDB string) {
	m.viewConverter.SetSourceDB(sourceDB)
}

// Close 关闭资源
func (m *Migrator) Close() error {
	var errs []error

	if m.mysqlClient != nil {
		if err := m.mysqlClient.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if m.oscarClient != nil {
		if err := m.oscarClient.Close(); err != nil {
			errs = append(errs, err)
		}
	}

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

// migrateTables 迁移表（四阶段：先创建所有表结构，再迁移所有数据，然后创建索引和外键，最后设置自增列）
func (m *Migrator) migrateTables(tables []string) *types.MigrationResult {
	result := &types.MigrationResult{}

	// 存储所有表的结构信息，供后续使用
	tableSchemas := make(map[string]*types.Table)

	// ========== 第一阶段：创建所有表结构 ==========
	log.Printf("========== 第一阶段：创建所有表结构 ==========")
	for i, tableName := range tables {
		log.Printf("[%d/%d] 处理表: %s", i+1, len(tables), tableName)

		// 1. 读取表结构
		table, err := m.schemaReader.GetTableSchema(tableName)
		if err != nil {
			result.TablesFailed++
			result.FailedTables = append(result.FailedTables, tableName)
			m.failedTableCreate[tableName] = true
			m.logger.LogTableCreateFailed(tableName, "", fmt.Sprintf("读取表结构失败: %v", err))
			continue
		}

		// 保存表结构供后续使用
		tableSchemas[tableName] = table

		// 记录自增列信息（稍后在第四阶段处理）
		for _, col := range table.Columns {
			if col.IsAutoIncr {
				m.autoIncrColumns = append(m.autoIncrColumns, autoIncrInfo{
					tableName:  tableName,
					columnName: col.Name,
				})
			}
		}

		// 2. 检查/删除已存在的表
		exists, err := m.oscarClient.TableExists(tableName)
		if err != nil {
			result.TablesFailed++
			result.FailedTables = append(result.FailedTables, tableName)
			m.failedTableCreate[tableName] = true
			m.logger.LogTableCreateFailed(tableName, "", fmt.Sprintf("检查表是否存在失败: %v", err))
			continue
		}

		if exists {
			if m.cfg.Migration.Overwrite {
				if err := m.oscarClient.DropTable(tableName); err != nil {
					result.TablesFailed++
					result.FailedTables = append(result.FailedTables, tableName)
					m.failedTableCreate[tableName] = true
					dropSQL := fmt.Sprintf("DROP TABLE %s", tableName)
					m.logger.LogTableCreateFailed(tableName, dropSQL, fmt.Sprintf("删除已存在的表失败: %v", err))
					continue
				}
			} else {
				result.TablesFailed++
				result.FailedTables = append(result.FailedTables, tableName)
				m.failedTableCreate[tableName] = true
				m.logger.LogTableCreateFailed(tableName, "", "表已存在")
				continue
			}
		}

		// 3. 创建表结构（不包含自增属性，稍后单独处理）
		sql, err := m.schemaWriter.CreateTableWithoutAutoIncr(table)
		if err != nil {
			result.TablesFailed++
			result.FailedTables = append(result.FailedTables, tableName)
			m.failedTableCreate[tableName] = true
			m.logger.LogTableCreateFailed(tableName, sql, fmt.Sprintf("%v", err))
			continue
		}

		// 4. 添加表注释
		if table.Comment != "" {
			_, err := m.schemaWriter.AddTableComment(tableName, table.Comment)
			if err != nil {
				log.Printf("[警告] 表 %s: 添加表注释失败: %v", tableName, err)
			}
		}

		// 5. 添加列注释
		failedColComments := m.schemaWriter.AddColumnComments(tableName, table.Columns)
		if len(failedColComments) > 0 {
			log.Printf("[警告] 表 %s: %d 个列注释添加失败", tableName, len(failedColComments))
		}

		log.Printf("[完成] 表 %s: 表结构创建成功", tableName)
	}

	// ========== 第二阶段：迁移所有表数据（并行） ==========
	log.Printf("========== 第二阶段：迁移所有表数据 ==========")

	// 过滤出成功创建的表（排除失败的表）
	successTables := make([]string, 0)
	for _, tableName := range tables {
		if !m.failedTableCreate[tableName] {
			successTables = append(successTables, tableName)
		}
	}

	// 使用 worker pool 并行迁移数据
	type dataResult struct {
		tableName string
		rowCount  int64
		err       error
		elapsed   time.Duration
	}

	jobs := make(chan string, len(successTables))
	results := make(chan *dataResult, len(successTables))

	var wg sync.WaitGroup

	// 启动 worker
	for i := 0; i < m.cfg.Migration.Parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tableName := range jobs {
				startTime := time.Now()
				table := tableSchemas[tableName]
				rowCount, err := m.migrateTableData(tableName, table.Columns)
				results <- &dataResult{
					tableName: tableName,
					rowCount:  rowCount,
					err:       err,
					elapsed:   time.Since(startTime),
				}
			}
		}()
	}

	// 发送任务
	for _, tableName := range successTables {
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
			m.logger.LogTableDataError(r.tableName, "", r.err.Error())
		} else {
			result.TablesMigrated++
			result.TotalRows += r.rowCount
			log.Printf("[完成] 表 %s: %d 行, 耗时 %v", r.tableName, r.rowCount, r.elapsed)
		}
	}

	// ========== 第三阶段：创建所有索引和外键 ==========
	log.Printf("========== 第三阶段：创建索引和外键 ==========")

	for i, tableName := range successTables {
		// 跳过数据迁移失败的表
		skip := false
		for _, failed := range result.FailedTables {
			if failed == tableName {
				skip = true
				break
			}
		}
		if skip {
			continue
		}

		table := tableSchemas[tableName]
		log.Printf("[%d/%d] 创建索引和外键: %s", i+1, len(successTables), tableName)

		// 创建索引
		if m.cfg.Migration.MigrateIndexes && len(table.Indexes) > 0 {
			failedIndexes := m.schemaWriter.CreateIndexes(tableName, table.Indexes)
			for _, fi := range failedIndexes {
				m.logger.LogIndexCreateFailed(tableName, fi.IndexName, fi.SQL, fi.Err.Error())
			}
			if len(failedIndexes) == 0 {
				log.Printf("[完成] 表 %s: 创建了 %d 个索引", tableName, len(table.Indexes))
			} else {
				log.Printf("[部分完成] 表 %s: 成功 %d 个索引, 失败 %d 个索引", tableName, len(table.Indexes)-len(failedIndexes), len(failedIndexes))
			}
		}

		// 创建外键
		if len(table.ForeignKeys) > 0 {
			for _, fk := range table.ForeignKeys {
				sql, err := m.schemaWriter.CreateSingleForeignKey(tableName, fk)
				if err != nil {
					m.logger.LogFkCreateFailed(tableName, fk.Name, sql, err.Error())
				}
			}
			log.Printf("[完成] 表 %s: 处理了 %d 个外键", tableName, len(table.ForeignKeys))
		}
	}

	// ========== 第四阶段：设置自增列 ==========
	log.Printf("========== 第四阶段：设置自增列 ==========")

	for _, ai := range m.autoIncrColumns {
		// 跳过失败的表
		if m.failedTableCreate[ai.tableName] {
			continue
		}

		log.Printf("[处理] 表 %s 自增列 %s", ai.tableName, ai.columnName)

		// 1. 先为自增列创建唯一索引
		sql, err := m.schemaWriter.CreateAutoIncrUniqueIndex(ai.tableName, ai.columnName)
		if err != nil {
			m.logger.LogAutoIncrFailed(ai.tableName, ai.columnName, sql, fmt.Sprintf("创建唯一索引失败: %v", err))
			continue
		}

		// 2. 修改列为自增列
		sql, err = m.schemaWriter.SetColumnAutoIncrement(ai.tableName, ai.columnName)
		if err != nil {
			m.logger.LogAutoIncrFailed(ai.tableName, ai.columnName, sql, fmt.Sprintf("设置自增属性失败: %v", err))
			continue
		}

		log.Printf("[完成] 表 %s 自增列 %s 设置成功", ai.tableName, ai.columnName)
	}

	return result
}

// migrateTableData 迁移表数据，返回迁移的行数
func (m *Migrator) migrateTableData(tableName string, columns []types.Column) (int64, error) {
	var totalRows int64

	// 获取列名
	colNames := make([]string, len(columns))
	for i, col := range columns {
		colNames[i] = col.Name
	}

	// 流式读取并批量写入
	err := m.dataReader.ReadTableData(tableName, colNames, func(batch *types.DataBatch) error {
		inserted, err := m.dataWriter.InsertBatchOptimized(tableName, batch)
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

// migrateViews 迁移视图
func (m *Migrator) migrateViews() *types.MigrationResult {
	result := &types.MigrationResult{}

	views, err := m.schemaReader.GetViews()
	if err != nil {
		log.Printf("获取视图列表失败: %v", err)
		return result
	}

	log.Printf("发现 %d 个视图需要迁移", len(views))

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
		exists, _ := m.oscarClient.TableExists(viewName)
		if exists {
			m.oscarClient.DropView(viewName)
		}

		// 创建视图
		sql, err := m.schemaWriter.CreateView(view)
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
