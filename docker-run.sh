#!/bin/bash
# 构建 Docker 镜像
docker build -t mysql2oscar .

# 运行容器（需要先配置 odbc.ini）
docker run --rm \
  -v $(pwd)/config.yaml:/app/config.yaml \
  -e LD_LIBRARY_PATH=/opt/oscar/odbc/lib \
  mysql2oscar \
  ./mysql2oscar -config config.yaml