# poolroute

poolroute 是一个反向代理网关的上游连接池与一致性哈希路由控制面。它维护上游节点
注册表、一致性哈希环、每节点连接池、主动健康探测、被动失败统计、熔断器和粘性会话，
并为流量分发提供一个确定性的 TCP 演示入口。

## 构建

amd64 / arm64 构建命令：

```bash
./build_benzhi_docker.sh poolroute linux/amd64
./build_benzhi_docker.sh poolroute linux/arm64
```

## 运行

```bash
go run ./cmd/poolroute -requests 120 -nodes 3
```

## 容器内验证

```bash
go build ./...
go test ./...
go vet ./...
go run ./cmd/poolroute -requests 60 -nodes 3
```

## 功能

- 上游节点注册表与生命周期状态机（healthy/suspected/unhealthy/draining/offline）
- 基于 murmur3 的一致性哈希环，权重虚拟节点与零权重占位
- 每节点连接池：获取、归还、排水
- 主动健康探测（真实 TCP 往返）与被动失败统计的合并健康视图
- 熔断器（closed/open/half-open）
- 粘性会话与失败重试
- 控制面快照与文本报表
