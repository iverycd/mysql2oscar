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
}
