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
	failedTableCreate sync.Map       // 表结构创建失败的表（线程��全）
	autoIncrColumns   []autoIncrInfo // 需要设置自增的列信息
	autoIncrMutex     sync.Mutex     // 保护 autoIncrColumns

	// 存储表结构供后续阶段使用
	tableSchemas   map[string]*types.Table
	tableSchemasMu sync.RWMutex
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
	// 考虑：表级并行度 × 分片并行度 + 缓冲
	chunkParallelism := cfg.Migration.ChunkParallelism
	if chunkParallelism < 1 {
		chunkParallelism = 2 // 默认值
	}
	maxConns := cfg.Migration.Parallelism*chunkParallelism + 5 // 缓冲

	log.Printf("[连接池] MySQL 最大连接数: %d (表并行=%d, 分片并行=%d)", maxConns, cfg.Migration.Parallelism, chunkParallelism)

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
		// 表结构缓存
		tableSchemas: make(map[string]*types.Table),
	}, nil
}

// SetSourceDB 设置源数据库（用于视图转换）
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

// migrateTableDataWithWriter 使用指定的 DataWriter 迁移表数据
func (m *Migrator) migrateTableDataWithWriter(tableName string, columns []types.Column, dataWriter *oscar.DataWriter, reconnectFunc func() (*oscar.Client, error)) (int64, error) {
	// 1. 获取表行数
	totalRows, err := m.dataReader.GetRowCount(tableName)
	if err != nil {
		log.Printf("[警告] 表 %s: 获取行数失败，使用单线程迁移: %v", tableName, err)
		totalRows = 0
	}

	// 2. 规划分片策略
	plan := m.planChunking(tableName, totalRows)

	// 3. 根据策略选择迁移方式
	if plan.Strategy == types.ChunkStrategyRange || plan.Strategy == types.ChunkStrategyOffset {
		strategyName := "整数主键范围分片"
		if plan.Strategy == types.ChunkStrategyOffset {
			strategyName = "字符串主键偏移分片"
		}
		log.Printf("[分片] 表 %s: 使用%s (行数: %d, 分片数: %d, 并行度: %d)",
			tableName, strategyName, totalRows, plan.NumChunks, m.cfg.Migration.ChunkParallelism)
		return m.migrateTableDataWithChunking(tableName, columns, dataWriter, plan)
	}

	// 降级为原有单线程
	log.Printf("[单线程] 表 %s: 使用单线程迁移 (行数: %d)", tableName, totalRows)
	return m.migrateTableDataSequential(tableName, columns, dataWriter, reconnectFunc)
}

// planChunking 规划分片策略
func (m *Migrator) planChunking(tableName string, totalRows int64) *types.ChunkPlan {
	plan := &types.ChunkPlan{
		Strategy: types.ChunkStrategyNone,
	}

	// 检查是否满足分片条件
	// 条件1：行数超过阈值
	if totalRows < m.cfg.Migration.ChunkThreshold {
		return plan
	}

	// 条件2：有主键
	pkInfo, err := m.schemaReader.GetPrimaryKeyInfo(tableName)
	if err != nil || pkInfo == nil {
		log.Printf("[分片] 表 %s: 无主键，使用单线程", tableName)
		return plan
	}

	// 获取分片大小
	chunkSize := m.cfg.Migration.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 10000
	}

	// 根据主键类型选择分片策略
	if pkInfo.IsInteger {
		// 整数主键：使用范围分片
		return m.planIntegerChunking(tableName, totalRows, pkInfo, chunkSize)
	} else if pkInfo.IsString {
		// 字符串主键：使用 OFFSET 分片
		return m.planOffsetChunking(tableName, totalRows, pkInfo, chunkSize)
	}

	log.Printf("[分片] 表 %s: 主键 '%s' 类型 '%s' 不支持分片，使用单线程", tableName, pkInfo.ColumnName, pkInfo.DataType)
	return plan
}

// planIntegerChunking 规划整数主键的分片策略
func (m *Migrator) planIntegerChunking(tableName string, totalRows int64, pkInfo *mysql.PrimaryKeyInfo, chunkSize int64) *types.ChunkPlan {
	plan := &types.ChunkPlan{
		Strategy:  types.ChunkStrategyNone,
		PKColumn:  pkInfo.ColumnName,
		ChunkSize: chunkSize,
	}

	// 获取主键范围
	minVal, maxVal, err := m.dataReader.GetPrimaryKeyRange(tableName, pkInfo.ColumnName)
	if err != nil {
		log.Printf("[分片] 表 %s: 获取主键范围失败: %v", tableName, err)
		return plan
	}

	// 空表
	if minVal == 0 && maxVal == 0 {
		return plan
	}

	// 计算分片数量
	rangeSize := maxVal - minVal + 1
	numChunks := int((rangeSize + chunkSize - 1) / chunkSize)
	if numChunks < 1 {
		numChunks = 1
	}

	// 生成分片范围
	chunks := make([]types.ChunkRange, 0, numChunks)
	for i := 0; i < numChunks; i++ {
		start := minVal + int64(i)*chunkSize
		end := minVal + int64(i+1)*chunkSize
		if end > maxVal+1 {
			end = maxVal + 1
		}
		chunks = append(chunks, types.ChunkRange{Start: start, End: end})
	}

	plan.Strategy = types.ChunkStrategyRange
	plan.MinValue = minVal
	plan.MaxValue = maxVal
	plan.NumChunks = numChunks
	plan.Chunks = chunks

	log.Printf("[分片] 表 %s: 整数主键分片 (范围: %d-%d, 分片数: %d)", tableName, minVal, maxVal, numChunks)
	return plan
}

// planOffsetChunking 规划字符串主键的分片策略
func (m *Migrator) planOffsetChunking(tableName string, totalRows int64, pkInfo *mysql.PrimaryKeyInfo, chunkSize int64) *types.ChunkPlan {
	plan := &types.ChunkPlan{
		Strategy:  types.ChunkStrategyNone,
		PKColumn:  pkInfo.ColumnName,
		ChunkSize: chunkSize,
	}

	// 计算分片数量
	numChunks := int((totalRows + chunkSize - 1) / chunkSize)
	if numChunks < 1 {
		numChunks = 1
	}

	// 生成偏移分片
	offsetChunks := make([]types.OffsetChunk, 0, numChunks)
	for i := 0; i < numChunks; i++ {
		offset := int64(i) * chunkSize
		offsetChunks = append(offsetChunks, types.OffsetChunk{
			ChunkID: i,
			Offset:  offset,
			Limit:   chunkSize,
		})
	}

	plan.Strategy = types.ChunkStrategyOffset
	plan.NumChunks = numChunks
	plan.OffsetChunks = offsetChunks

	log.Printf("[分片] 表 %s: 字符串主键分片 (总行数: %d, 分片数: %d)", tableName, totalRows, numChunks)
	return plan
}

// migrateTableDataSequential 单线程顺序迁移（原有逻辑）
func (m *Migrator) migrateTableDataSequential(tableName string, columns []types.Column, dataWriter *oscar.DataWriter, reconnectFunc func() (*oscar.Client, error)) (int64, error) {
	var totalRows int64

	// 获取列名
	colNames := make([]string, len(columns))
	for i, col := range columns {
		colNames[i] = col.Name
	}

	// 流式读取并批量写入
	err := m.dataReader.ReadTableData(tableName, colNames, func(batch *types.DataBatch) error {
		// 使用带重试的批量插入，最多重试3次（支持连接断开后重连）
		inserted, err := dataWriter.InsertBatchWithRetry(tableName, batch, 3, reconnectFunc)
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

// chunkResult 分片迁移结果
type chunkResult struct {
	chunkID int
	rows    int64
	err     error
}

// migrateTableDataWithChunking 分片并行迁移
func (m *Migrator) migrateTableDataWithChunking(tableName string, columns []types.Column, dataWriter *oscar.DataWriter, plan *types.ChunkPlan) (int64, error) {
	// 获取列名
	colNames := make([]string, len(columns))
	for i, col := range columns {
		colNames[i] = col.Name
	}

	// 根据分片策略选择执行方式
	switch plan.Strategy {
	case types.ChunkStrategyRange:
		return m.migrateWithRangeChunking(tableName, colNames, dataWriter, plan)
	case types.ChunkStrategyOffset:
		return m.migrateWithOffsetChunking(tableName, colNames, dataWriter, plan)
	default:
		return 0, fmt.Errorf("未知的分片策略: %v", plan.Strategy)
	}
}

// migrateWithRangeChunking 整数主键范围分片迁移
func (m *Migrator) migrateWithRangeChunking(tableName string, colNames []string, dataWriter *oscar.DataWriter, plan *types.ChunkPlan) (int64, error) {
	// 创建分片任务通道
	chunkChan := make(chan types.ChunkRange, len(plan.Chunks))
	resultChan := make(chan chunkResult, len(plan.Chunks))

	// 发送所有分片任务
	for _, chunk := range plan.Chunks {
		chunkChan <- chunk
	}
	close(chunkChan)

	// 并行处理分片
	parallelism := m.cfg.Migration.ChunkParallelism
	if parallelism < 1 {
		parallelism = 2
	}
	if parallelism > len(plan.Chunks) {
		parallelism = len(plan.Chunks)
	}

	var wg sync.WaitGroup
	var totalRows int64
	var firstError error
	var errorMutex sync.Mutex

	// 启动 worker（每个分片使用独立连接）
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for chunk := range chunkChan {
				// ========== 每个分片独立连接 + 分片级别重试 ==========

				var chunkRows int64
				var chunkErr error
				maxChunkRetries := 3 // 分片级别最大重试次数

				for retry := 0; retry < maxChunkRetries; retry++ {
					if retry > 0 {
						log.Printf("[Worker-%d] 表 %s 分片[%d,%d) 第 %d 次重试...",
							workerID, tableName, chunk.Start, chunk.End, retry)
						time.Sleep(time.Duration(retry) * time.Second) // 指数退避
					}

					// 1. 创建分片独立连接
					chunkClient, err := m.createTempClient()
					if err != nil {
						chunkErr = err
						continue // 连接创建失败，重试
					}

					// 2. 创建分片独立 DataWriter
					chunkDataWriter := oscar.NewDataWriter(chunkClient)

					// 3. 定义分片级别的重连函数
					currentClient := chunkClient
					reconnectFunc := func() (*oscar.Client, error) {
						if currentClient != nil {
							currentClient.Close()
						}
						newClient, err := m.createTempClient()
						if err != nil {
							return nil, err
						}
						currentClient = newClient
						chunkDataWriter.SetClient(currentClient)
						log.Printf("[Worker-%d][分片 %d,%d] 批次重连成功", workerID, chunk.Start, chunk.End)
						return currentClient, nil
					}

					// 4. 处理分片数据
					chunkRows = 0
					chunkErr = m.dataReader.ReadTableDataByRange(tableName, colNames, plan.PKColumn,
						chunk.Start, chunk.End, func(batch *types.DataBatch) error {
							inserted, err := chunkDataWriter.InsertBatchWithRetry(tableName, batch, 5, reconnectFunc) // 增加批次重试次数到 5
							if err != nil {
								return err
							}
							chunkRows += inserted
							atomic.AddInt64(&totalRows, inserted)

							if chunkRows > 0 && chunkRows%5000 == 0 {
								log.Printf("[Worker-%d] 表 %s 分片[%d,%d): 已插入 %d 行",
									workerID, tableName, chunk.Start, chunk.End, chunkRows)
							}
							return nil
						})

					// 5. 关闭分片连接
					if currentClient != nil {
						currentClient.Close()
					}

					// 6. 如果成功，跳出重试循环
					if chunkErr == nil {
						break
					}

					log.Printf("[Worker-%d] 表 %s 分片[%d,%d) 失败: %v",
						workerID, tableName, chunk.Start, chunk.End, chunkErr)
				}

				result := chunkResult{rows: chunkRows, err: chunkErr}
				resultChan <- result

				if chunkErr != nil {
					errorMutex.Lock()
					if firstError == nil {
						firstError = fmt.Errorf("分片 [%d, %d) 失败 (重试 %d 次后): %w",
							chunk.Start, chunk.End, maxChunkRetries, chunkErr)
					}
					errorMutex.Unlock()
					return
				}

				log.Printf("[Worker-%d] 表 %s 分片[%d,%d) 完成: %d 行",
					workerID, tableName, chunk.Start, chunk.End, chunkRows)
			}
		}(i)
	}

	// 等待所有 worker 完成
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// 收集结果
	for range resultChan {
	}

	if firstError != nil {
		return totalRows, firstError
	}

	log.Printf("[完成] 表 %s: 范围分片迁移完成，共 %d 行", tableName, totalRows)
	return totalRows, nil
}

// migrateWithOffsetChunking 字符串主键偏移分片迁移
func (m *Migrator) migrateWithOffsetChunking(tableName string, colNames []string, dataWriter *oscar.DataWriter, plan *types.ChunkPlan) (int64, error) {
	// 创建分片任务通道
	chunkChan := make(chan types.OffsetChunk, len(plan.OffsetChunks))
	resultChan := make(chan chunkResult, len(plan.OffsetChunks))

	// 发送所有分片任务
	for _, chunk := range plan.OffsetChunks {
		chunkChan <- chunk
	}
	close(chunkChan)

	// 并行处理分片
	parallelism := m.cfg.Migration.ChunkParallelism
	if parallelism < 1 {
		parallelism = 2
	}
	if parallelism > len(plan.OffsetChunks) {
		parallelism = len(plan.OffsetChunks)
	}

	var wg sync.WaitGroup
	var totalRows int64
	var firstError error
	var errorMutex sync.Mutex

	// 启动 worker（每个分片使用独立连接）
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for chunk := range chunkChan {
				// ========== 每个分片独立连接 + 分片级别重试 ==========

				var chunkRows int64
				var chunkErr error
				maxChunkRetries := 3 // 分片级别最大重试次数

				for retry := 0; retry < maxChunkRetries; retry++ {
					if retry > 0 {
						log.Printf("[Worker-%d] 表 %s 分片#%d(offset=%d) 第 %d 次重试...",
							workerID, tableName, chunk.ChunkID, chunk.Offset, retry)
						time.Sleep(time.Duration(retry) * time.Second) // 指数退避
					}

					// 1. 创建分片独立连接
					chunkClient, err := m.createTempClient()
					if err != nil {
						chunkErr = err
						continue // 连接创建失败，重试
					}

					// 2. 创建分片独立 DataWriter
					chunkDataWriter := oscar.NewDataWriter(chunkClient)

					// 3. 定义分片级别的重连函数
					currentClient := chunkClient
					reconnectFunc := func() (*oscar.Client, error) {
						if currentClient != nil {
							currentClient.Close()
						}
						newClient, err := m.createTempClient()
						if err != nil {
							return nil, err
						}
						currentClient = newClient
						chunkDataWriter.SetClient(currentClient)
						log.Printf("[Worker-%d][分片#%d] 批次重连成功", workerID, chunk.ChunkID)
						return currentClient, nil
					}

					// 4. 处理分片数据
					chunkRows = 0
					chunkErr = m.dataReader.ReadTableDataByOffset(tableName, colNames, plan.PKColumn,
						chunk.Offset, chunk.Limit, func(batch *types.DataBatch) error {
							inserted, err := chunkDataWriter.InsertBatchWithRetry(tableName, batch, 5, reconnectFunc) // 增加批次重试次数到 5
							if err != nil {
								return err
							}
							chunkRows += inserted
							atomic.AddInt64(&totalRows, inserted)

							if chunkRows > 0 && chunkRows%5000 == 0 {
								log.Printf("[Worker-%d] 表 %s 分片#%d(offset=%d): 已插入 %d 行",
									workerID, tableName, chunk.ChunkID, chunk.Offset, chunkRows)
							}
							return nil
						})

					// 5. 关闭分片连接
					if currentClient != nil {
						currentClient.Close()
					}

					// 6. 如果成功，跳出重试循环
					if chunkErr == nil {
						break
					}

					log.Printf("[Worker-%d] 表 %s 分片#%d(offset=%d) 失败: %v",
						workerID, tableName, chunk.ChunkID, chunk.Offset, chunkErr)
				}

				result := chunkResult{chunkID: chunk.ChunkID, rows: chunkRows, err: chunkErr}
				resultChan <- result

				if chunkErr != nil {
					errorMutex.Lock()
					if firstError == nil {
						firstError = fmt.Errorf("偏移分片 %d (offset=%d) 失败 (重试 %d 次后): %w",
							chunk.ChunkID, chunk.Offset, maxChunkRetries, chunkErr)
					}
					errorMutex.Unlock()
					return
				}

				log.Printf("[Worker-%d] 表 %s 分片#%d(offset=%d) 完成: %d 行",
					workerID, tableName, chunk.ChunkID, chunk.Offset, chunkRows)
			}
		}(i)
	}

	// 等待所有 worker 完成
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// 收集结果
	for range resultChan {
	}

	if firstError != nil {
		return totalRows, firstError
	}

	log.Printf("[完成] 表 %s: 偏移分片迁移完成，共 %d 行", tableName, totalRows)
	return totalRows, nil
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

// Migrate 执行迁移（三阶段模式）
func (m *Migrator) Migrate() (*types.MigrationResult, error) {
	startTime := time.Now()
	result := &types.MigrationResult{}

	// 获取要迁移的表列表
	tables, err := m.getTablesToMigrate()
	if err != nil {
		return nil, err
	}

	log.Printf("发现 %d 个表需要迁移", len(tables))

	// 预先获取所有表的自增列信息（包含起始值）
	autoIncrInfoMap, err := m.schemaReader.GetAutoIncrementInfo()
	if err != nil {
		log.Printf("[注意] 获取自增列信息失败: %v，将使用默认起始值1", err)
		autoIncrInfoMap = make(map[string]mysql.AutoIncrementInfo)
	}

	// ========== 第一阶段: 单线程创建所有表结构 ==========
	log.Printf("========== 第一阶段: 创建所有表结构 ==========")
	schemaResults := m.createAllTableStructures(tables)
	successfulTables := m.collectSuccessfulTables(schemaResults)
	m.storeTableSchemas(schemaResults)

	// 统计第一阶段结果
	for _, sr := range schemaResults {
		if sr.err != nil {
			result.TablesFailed++
			result.FailedTables = append(result.FailedTables, sr.tableName)
		} else {
			result.TablesMigrated++
		}
	}

	// ========== 第二阶段: 并行迁移所有表数据 ==========
	log.Printf("========== 第二阶段: 迁移所有表数据 ==========")
	dataResults := m.migrateAllTableData(successfulTables)

	// 统计第二阶段结果
	for _, dr := range dataResults {
		if dr.err != nil {
			m.logger.LogTableDataError(dr.tableName, "", dr.errMsg)
			result.FailedDataTables = append(result.FailedDataTables, dr.tableName)
		} else {
			result.TotalRows += dr.rowCount
		}
	}

	// ========== 第三阶段: 创建索引/约束/自增列 ==========
	log.Printf("========== 第三阶段: 创建索引/约束/自增列 ==========")
	postDataResult := m.createAllPostDataObjects(successfulTables, autoIncrInfoMap)

	// 统计第三阶段结果
	// 主键属于约束，成功/失败各计1
	if postDataResult.pkSuccess {
		result.ConstraintsSuccess++
	}
	if postDataResult.pkFailed {
		result.ConstraintsFailed++
	}
	// 索引
	result.IndexesSuccess += postDataResult.indexesSuccess
	result.IndexesFailed += postDataResult.indexesFailed
	// 外键属于约束
	result.ConstraintsSuccess += postDataResult.fkSuccess
	result.ConstraintsFailed += postDataResult.fkFailed
	// 自增列
	result.AutoIncrSuccess += postDataResult.autoIncrSuccess
	result.AutoIncrFailed += postDataResult.autoIncrFailed

	// ========== 第四阶段: 迁移视图 ==========
	if m.cfg.Migration.MigrateViews {
		log.Printf("========== 第四阶段: 迁移视图 ==========")
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

// tableDataResult 表数据迁移结果
type tableDataResult struct {
	tableName string
	rowCount  int64
	err       error
	errMsg    string
	elapsed   time.Duration
}

// postDataResult 后数据对象创建结果
type postDataResult struct {
	tableName       string
	pkSuccess       bool // 主键创建成功
	pkFailed        bool // 主键创建失败
	indexesSuccess  int  // 索引创建成功数
	indexesFailed   int  // 索引创建失败数
	fkSuccess       int  // 外键创建成功数
	fkFailed        int  // 外键创建失败数
	autoIncrSuccess int  // 自增列设置成功数
	autoIncrFailed  int  // 自增列设置失败数
}

// ========== 第一阶段: 创建所有表结构 ==========

// createAllTableStructures 第一阶段：单线程创建所有表结构
func (m *Migrator) createAllTableStructures(tables []string) []*schemaResult {
	results := make([]*schemaResult, len(tables))

	// 创建单个连接用于所有表
	client, err := m.createTempClient()
	if err != nil {
		// 所有表标记为失败
		for i, tableName := range tables {
			results[i] = &schemaResult{
				tableName: tableName,
				err:       err,
				errMsg:    fmt.Sprintf("创建 Oscar 连接失败: %v", err),
			}
			m.failedTableCreate.Store(tableName, true)
			m.logger.LogTableCreateFailed(tableName, "", fmt.Sprintf("创建 Oscar 连接失败: %v", err))
		}
		return results
	}
	defer client.Close()

	schemaWriter := oscar.NewSchemaWriter(client)

	for i, tableName := range tables {
		log.Printf("[%d/%d] 创建表结构: %s", i+1, len(tables), tableName)
		startTime := time.Now()
		result := m.createSingleTableStructure(tableName, schemaWriter, client)
		result.elapsed = time.Since(startTime)
		results[i] = result

		if result.err != nil {
			m.failedTableCreate.Store(tableName, true)
			if result.errSQL != "" {
				m.logger.LogTableCreateFailed(tableName, result.errSQL, result.errMsg)
			}
			log.Printf("[%d/%d] 表 %s: 创建失败 - %s", i+1, len(tables), tableName, result.errMsg)
		} else {
			log.Printf("[%d/%d] 表 %s: 创建成功 (%v)", i+1, len(tables), tableName, result.elapsed)
		}
	}
	return results
}

// createSingleTableStructure 创建单个表的结构（不含数据和索引）
func (m *Migrator) createSingleTableStructure(tableName string, schemaWriter *oscar.SchemaWriter, client *oscar.Client) *schemaResult {
	result := &schemaResult{tableName: tableName}

	// 1. 读取表结构
	table, err := m.schemaReader.GetTableSchema(tableName)
	if err != nil {
		result.err = err
		result.errMsg = fmt.Sprintf("读取表结构失败: %v", err)
		return result
	}
	result.table = table

	// 2. 处理已存在的表
	if m.cfg.Migration.Overwrite {
		if err := client.DropTable(tableName); err != nil {
			result.err = err
			result.errMsg = fmt.Sprintf("删除表失败: %v", err)
			result.errSQL = fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
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

	// 3. 创建表结构（不包含自增属性）
	sql, err := schemaWriter.CreateTableWithoutAutoIncr(table)
	if err != nil {
		result.err = err
		result.errMsg = fmt.Sprintf("%v", err)
		result.errSQL = sql
		return result
	}

	// 4. 添加表注释
	if table.Comment != "" {
		_, err := schemaWriter.AddTableComment(tableName, table.Comment)
		if err != nil {
			log.Printf("[警告] 表 %s: 添加表注释失败: %v", tableName, err)
		}
	}

	// 5. 添加列注释
	failedColComments := schemaWriter.AddColumnComments(tableName, table.Columns)
	if len(failedColComments) > 0 {
		log.Printf("[警告] 表 %s: %d 个列注释添加失败", tableName, len(failedColComments))
	}

	return result
}

// ========== 第二阶段: 迁移所有表数据 ==========

// migrateAllTableData 第二阶段：并行迁移所有表数据
func (m *Migrator) migrateAllTableData(tables []string) []*tableDataResult {
	results := make([]*tableDataResult, len(tables))

	if len(tables) == 0 {
		return results
	}

	// 使用 worker pool 并行迁移数据
	jobs := make(chan string, len(tables))
	resultChan := make(chan *tableDataResult, len(tables))

	var wg sync.WaitGroup
	var completedCount int64

	// 启动 worker
	for i := 0; i < m.cfg.Migration.Parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tableName := range jobs {
				startTime := time.Now()
				r := m.migrateTableDataOnly(tableName)
				r.elapsed = time.Since(startTime)

				// 更新进度计数
				current := atomic.AddInt64(&completedCount, 1)
				if r.err != nil {
					log.Printf("[数据 %d/%d] 表 %s: 失败 - %s (%v)", current, len(tables), tableName, r.errMsg, r.elapsed)
				} else {
					log.Printf("[数据 %d/%d] 表 %s: 成功 - %d 行 (%v)", current, len(tables), tableName, r.rowCount, r.elapsed)
				}

				resultChan <- r
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
		close(resultChan)
	}()

	// 收集结果（保持顺序）
	tableIndexMap := make(map[string]int)
	for i, t := range tables {
		tableIndexMap[t] = i
	}
	for r := range resultChan {
		if idx, ok := tableIndexMap[r.tableName]; ok {
			results[idx] = r
		}
	}

	return results
}

// migrateTableDataOnly 仅迁移表数据
func (m *Migrator) migrateTableDataOnly(tableName string) *tableDataResult {
	result := &tableDataResult{tableName: tableName}

	// 1. 获取缓存的表结构
	m.tableSchemasMu.RLock()
	table, ok := m.tableSchemas[tableName]
	m.tableSchemasMu.RUnlock()

	if !ok {
		result.err = fmt.Errorf("表结构未找到")
		result.errMsg = "表结构未找到"
		return result
	}

	// 2. 创建连接
	client, err := m.createTempClient()
	if err != nil {
		result.err = err
		result.errMsg = fmt.Sprintf("创建 Oscar 连接失败: %v", err)
		return result
	}
	defer client.Close()

	// 3. 创建 DataWriter
	dataWriter := oscar.NewDataWriter(client)

	// 4. 定义重建连接函数
	reconnectFunc := func() (*oscar.Client, error) {
		if client != nil {
			client.Close()
		}
		newClient, err := m.createTempClient()
		if err != nil {
			return nil, err
		}
		client = newClient
		dataWriter.SetClient(client)
		log.Printf("[表 %s] 重建连接成功", tableName)
		return client, nil
	}

	// 5. 迁移数据
	rowCount, err := m.migrateTableDataWithWriter(tableName, table.Columns, dataWriter, reconnectFunc)
	if err != nil {
		result.err = err
		result.errMsg = fmt.Sprintf("迁移数据失败: %v", err)
		return result
	}
	result.rowCount = rowCount

	return result
}

// ========== 第三阶段: 创建索引/约束/自增列 ==========

// createAllPostDataObjects 第三阶段：串行创建索引、约束、自增列
func (m *Migrator) createAllPostDataObjects(tables []string, autoIncrInfoMap map[string]mysql.AutoIncrementInfo) *postDataResult {
	totalResult := &postDataResult{}

	if len(tables) == 0 {
		return totalResult
	}

	// 创建单个连接
	client, err := m.createTempClient()
	if err != nil {
		log.Printf("[错误] 创建 Oscar 连接失败: %v", err)
		return totalResult
	}
	defer client.Close()

	schemaWriter := oscar.NewSchemaWriter(client)

	for i, tableName := range tables {
		log.Printf("[%d/%d] 创建索引/约束: %s", i+1, len(tables), tableName)
		result := m.createSingleTablePostDataObjects(tableName, autoIncrInfoMap, schemaWriter)
		// 汇总统计
		if result.pkSuccess {
			totalResult.pkSuccess = true
		}
		if result.pkFailed {
			totalResult.pkFailed = true
		}
		totalResult.indexesSuccess += result.indexesSuccess
		totalResult.indexesFailed += result.indexesFailed
		totalResult.fkSuccess += result.fkSuccess
		totalResult.fkFailed += result.fkFailed
		totalResult.autoIncrSuccess += result.autoIncrSuccess
		totalResult.autoIncrFailed += result.autoIncrFailed
	}

	return totalResult
}

// createSingleTablePostDataObjects 创建单个表的后数据对象（主键、索引、外键、自增列）
func (m *Migrator) createSingleTablePostDataObjects(tableName string, autoIncrInfoMap map[string]mysql.AutoIncrementInfo, schemaWriter *oscar.SchemaWriter) *postDataResult {
	result := &postDataResult{tableName: tableName}

	// 获取缓存的表结构
	m.tableSchemasMu.RLock()
	table, ok := m.tableSchemas[tableName]
	m.tableSchemasMu.RUnlock()

	if !ok {
		log.Printf("[警告] 表 %s: 表结构未找到，跳过后数据对象创建", tableName)
		return result
	}

	// 1. 创建主键
	for _, idx := range table.Indexes {
		if idx.IsPrimary {
			sql, err := schemaWriter.AddPrimaryKey(tableName, idx.Columns)
			if err != nil {
				m.logger.LogIndexCreateFailed(tableName, "PRIMARY", sql, err.Error())
				log.Printf("[警告] 表 %s: 添加主键失败: %v", tableName, err)
				result.pkFailed = true
			} else {
				log.Printf("[完成] 表 %s: 添加主键 (%s)", tableName, idx.Name)
				result.pkSuccess = true
			}
			break // 每个表只有一个主键
		}
	}

	// 2. 创建索引
	if m.cfg.Migration.MigrateIndexes && len(table.Indexes) > 0 {
		failedIndexes := schemaWriter.CreateIndexes(tableName, table.Indexes)
		for _, fi := range failedIndexes {
			m.logger.LogIndexCreateFailed(tableName, fi.IndexName, fi.SQL, fi.Err.Error())
		}
		result.indexesSuccess = len(table.Indexes) - len(failedIndexes)
		result.indexesFailed = len(failedIndexes)
		if len(failedIndexes) == 0 {
			log.Printf("[完成] 表 %s: 创建了 %d 个索引", tableName, len(table.Indexes))
		} else {
			log.Printf("[部分完成] 表 %s: 成功 %d 个索引, 失败 %d 个索引", tableName, result.indexesSuccess, result.indexesFailed)
		}
	}

	// 3. 创建外键
	if len(table.ForeignKeys) > 0 {
		for _, fk := range table.ForeignKeys {
			sql, err := schemaWriter.CreateSingleForeignKey(tableName, fk)
			if err != nil {
				m.logger.LogFkCreateFailed(tableName, fk.Name, sql, err.Error())
				result.fkFailed++
			} else {
				result.fkSuccess++
			}
		}
		log.Printf("[完成] 表 %s: 处理了 %d 个外键 (成功: %d, 失败: %d)", tableName, len(table.ForeignKeys), result.fkSuccess, result.fkFailed)
	}

	// 4. 设置自增列（如果有）
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
				result.autoIncrFailed++
				continue
			}

			// 设置列的默认值为序列的下一个值
			sql, err = schemaWriter.SetColumnDefaultSequence(tableName, col.Name)
			if err != nil {
				m.logger.LogAutoIncrFailed(tableName, col.Name, sql, fmt.Sprintf("设置列默认值为序列失败: %v", err))
				log.Printf("[警告] 表 %s 自增列 %s: 设置默认值失败: %v", tableName, col.Name, err)
				result.autoIncrFailed++
				continue
			}

			result.autoIncrSuccess++
			log.Printf("[完成] 表 %s 自增列 %s 设置成功 (起始值: %d)", tableName, col.Name, startValue)
		}
	}

	return result
}

// ========== 辅助函数 ==========

// collectSuccessfulTables 从结果中收集成功的表名
func (m *Migrator) collectSuccessfulTables(results []*schemaResult) []string {
	var tables []string
	for _, r := range results {
		if r.err == nil {
			tables = append(tables, r.tableName)
		}
	}
	return tables
}

// storeTableSchemas 存储表结构供后续阶段使用
func (m *Migrator) storeTableSchemas(results []*schemaResult) {
	m.tableSchemasMu.Lock()
	defer m.tableSchemasMu.Unlock()

	for _, r := range results {
		if r.table != nil {
			m.tableSchemas[r.tableName] = r.table
		}
	}
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
		//exists, _ := client.TableExists(viewName)
		//if exists {
		//	client.DropView(viewName)
		//}

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
