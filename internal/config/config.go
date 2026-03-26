package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// Config 表示整个配置文件
type Config struct {
	Source    MySQLConfig     `yaml:"source"`
	Target    OscarConfig     `yaml:"target"`
	Migration MigrationConfig `yaml:"migration"`
}

// MySQLConfig 表示 MySQL 源数据库配置
type MySQLConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	Charset  string `yaml:"charset"`
}

// OscarConfig 表示 Oscar 目标数据库配置
type OscarConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

// MigrationConfig 表示迁移配置
type MigrationConfig struct {
	// 要迁移的表列表，为空表示迁移整个数据库
	Tables []string `yaml:"tables"`
	// 是否迁移视图
	MigrateViews bool `yaml:"migrate_views"`
	// 是否迁移索引
	MigrateIndexes bool `yaml:"migrate_indexes"`
	// 并行数
	Parallelism int `yaml:"parallelism"`
	// 批处理大小
	BatchSize int `yaml:"batch_size"`
	// 是否覆盖已存在的表
	Overwrite bool `yaml:"overwrite"`

	// 分片配置
	// 是否启用分片迁移（默认 true）
	EnableChunking bool `yaml:"enable_chunking"`
	// 每个分片的行数（默认 10000）
	ChunkSize int64 `yaml:"chunk_size"`
	// 单表内分片并行度（默认 2）
	ChunkParallelism int `yaml:"chunk_parallelism"`
	// 启用分片的阈值行数，小于此值的表不分片（默认 50000）
	ChunkThreshold int64 `yaml:"chunk_threshold"`

	// 是否使用大写标识符（表名、列名、索引名、序列名、外键名等）
	// false: 使用小写（默认）
	// true: 使用大写
	UseUppercase bool `yaml:"use_uppercase"`
}

// Load 从文件加载配置
func Load(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	// 设置默认值
	setDefaults(&cfg)

	return &cfg, nil
}

// setDefaults 设置配置默认值
func setDefaults(cfg *Config) {
	if cfg.Source.Port == 0 {
		cfg.Source.Port = 3306
	}
	if cfg.Source.Charset == "" {
		cfg.Source.Charset = "utf8mb4"
	}
	if cfg.Migration.Parallelism == 0 {
		cfg.Migration.Parallelism = 4
	}
	if cfg.Migration.BatchSize == 0 {
		cfg.Migration.BatchSize = 1000
	}
	// 分片配置默认值
	// 注意：EnableChunking 默认为 false（零值），启用需要显式设置为 true
	// 这里通过检查 ChunkSize == 0 来判断是否需要设置默认值
	if cfg.Migration.ChunkSize == 0 {
		cfg.Migration.ChunkSize = 10000
	}
	if cfg.Migration.ChunkParallelism == 0 {
		cfg.Migration.ChunkParallelism = 2
	}
	if cfg.Migration.ChunkThreshold == 0 {
		cfg.Migration.ChunkThreshold = 50000
	}

}
