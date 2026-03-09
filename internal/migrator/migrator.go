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
}

// New 创建迁移器
func New(cfg *config.Config) (*Migrator, error) {
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
		return nil, fmt.Errorf("创建 Oscar 客户端失败: %w", err)
	}

	return &Migrator{
		cfg:           cfg,
		mysqlClient:   mysqlClient,
		oscarClient:   oscarClient,
		schemaReader:  mysql.NewSchemaReader(mysqlClient),
		dataReader:    mysql.NewDataReader(mysqlClient, cfg.Migration.BatchSize),
		schemaWriter:  oscar.NewSchemaWriter(oscarClient),
		dataWriter:    oscar.NewDataWriter(oscarClient),
		typeMapper:    transform.NewTypeMapper(),
		viewConverter: transform.NewViewConverter(),
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

	// 迁移表结构��数据
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

// migrateTables 迁移表（两阶段：先创建所有表结构，再迁移所有数据，最后创建外键）
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
			log.Printf("[失败] 表 %s: 读取表结构失败: %v", tableName, err)
			continue
		}

		// 保存表结构供后续使用
		tableSchemas[tableName] = table

		// 2. 检查/删除已存在的表
		exists, err := m.oscarClient.TableExists(tableName)
		if err != nil {
			result.TablesFailed++
			result.FailedTables = append(result.FailedTables, tableName)
			log.Printf("[失败] 表 %s: 检查表是否存在失败: %v", tableName, err)
			continue
		}

		if exists {
			if m.cfg.Migration.Overwrite {
				if err := m.oscarClient.DropTable(tableName); err != nil {
					result.TablesFailed++
					result.FailedTables = append(result.FailedTables, tableName)
					log.Printf("[失败] 表 %s: 删除已存在的表失败: %v", tableName, err)
					continue
				}
			} else {
				result.TablesFailed++
				result.FailedTables = append(result.FailedTables, tableName)
				log.Printf("[失败] 表 %s: 表已存在", tableName)
				continue
			}
		}

		// 3. 创建表结构
		if err := m.schemaWriter.CreateTable(table); err != nil {
			result.TablesFailed++
			result.FailedTables = append(result.FailedTables, tableName)
			log.Printf("[失败] 表 %s: 创建表失败: %v", tableName, err)
			continue
		}

		log.Printf("[完成] 表 %s: 表结构创建成功", tableName)
	}

	// ========== 第二阶段：迁移所有表数据（并行） ==========
	log.Printf("========== 第二阶段：迁移所有表数据 ==========")

	// 过滤出成功创建的表
	successTables := make([]string, 0)
	for _, tableName := range tables {
		if _, ok := tableSchemas[tableName]; ok {
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
			log.Printf("[失败] 表 %s: 迁移数据失败: %v", r.tableName, r.err)
		} else {
			result.TablesMigrated++
			result.TotalRows += r.rowCount
			log.Printf("[完成] 表 %s: %d 行, 耗时 %v", r.tableName, r.rowCount, r.elapsed)
		}
	}

	// ========== 第三阶段：创建所有索引和外键 ==========
	log.Printf("========== 第三阶段：创建索引和外键 ==========")

	for i, tableName := range successTables {
		table := tableSchemas[tableName]
		log.Printf("[%d/%d] 创建索引和外键: %s", i+1, len(successTables), tableName)

		// 创建索引
		if m.cfg.Migration.MigrateIndexes && len(table.Indexes) > 0 {
			if err := m.schemaWriter.CreateIndexes(tableName, table.Indexes); err != nil {
				log.Printf("[警告] 表 %s 创建索引失败: %v", tableName, err)
			} else {
				log.Printf("[完成] 表 %s: 创建了 %d 个索引", tableName, len(table.Indexes))
			}
		}

		// 创建外键
		if len(table.ForeignKeys) > 0 {
			if err := m.schemaWriter.CreateForeignKeys(tableName, table.ForeignKeys); err != nil {
				log.Printf("[警告] 表 %s 创建外键失败: %v", tableName, err)
			} else {
				log.Printf("[完成] 表 %s: 创建了 %d 个外键", tableName, len(table.ForeignKeys))
			}
		}
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
			log.Printf("[失败] 视图 %s: %v", viewName, err)
			continue
		}

		// 转换视图 SQL
		view.Definition = m.viewConverter.ConvertViewSQL(view.Definition)
		//originalSQL := view.Definition
		//log.Printf("[调试] 视图 %s 原始SQL: %s", viewName, originalSQL)
		//log.Printf("[调试] 视图 %s 转换后SQL: %s", viewName, view.Definition)

		// 检查视图是否存在
		exists, _ := m.oscarClient.TableExists(viewName)
		if exists {
			m.oscarClient.DropView(viewName)
		}

		// 创建视图
		if err := m.schemaWriter.CreateView(view); err != nil {
			result.ViewsFailed++
			result.FailedViews = append(result.FailedViews, viewName)
			log.Printf("[失败] 视图 %s: %v", viewName, err)
			continue
		}

		result.ViewsMigrated++
		log.Printf("[完成] 视图 %s", viewName)
	}

	return result
}
