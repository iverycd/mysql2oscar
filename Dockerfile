FROM golang:1.21-bookworm

# 安装 unixODBC
RUN apt-get update && apt-get install -y \
    unixodbc \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 设置 Go 代理
ENV GOPROXY=https://goproxy.cn,direct

# 复制 go.mod 先（利用缓存）
COPY go.mod ./
RUN go mod download || true

# 复制源代码
COPY . .

# 构建
RUN go mod tidy && go build -o mysql2oscar ./cmd/mysql2oscar

# 创建 ODBC 驱动目录
RUN mkdir -p /opt/oscar/odbc

# 设置库路径
ENV LD_LIBRARY_PATH=/opt/oscar/odbc/lib:$LD_LIBRARY_PATH

# 入口
ENTRYPOINT ["./mysql2oscar"]
CMD ["-config", "config.yaml"]