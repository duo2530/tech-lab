# RabbitMQ Lab 01：订单异步处理与路由模型

验证 RabbitMQ 四类交换机（Direct / Fanout / Topic / Headers）的路由行为，并对比“同步调用下游”与“异步发布事件”的下单耗时。

- 实验报告（架构图、时序图、实际结果、结论）：[docs/experiment-report.md](docs/experiment-report.md)
- 测试运行日志：[docs/logs/](docs/logs/)
- 路线图：[../README.md](../README.md)

## 环境

| 组件 | 版本 |
|---|---|
| JDK | 17 |
| Spring Boot / Spring AMQP | 3.0.5 / 3.0.x（与仓库 `distributed-lock` 模块一致） |
| RabbitMQ | `rabbitmq:4.1.4-management`（固定标签） |
| Docker | Docker Desktop 29.x（Compose v2） |

## 运行

以下命令均在 `rabbitmq/lab-01-order-routing/` 目录下执行。

```bash
# 1. 启动 RabbitMQ（等待健康检查通过）
docker compose up -d --wait

# 2. 运行全部测试（需要 Broker 已启动；测试可重复运行，每次开始前会清空相关队列）
mvn test

# 也可以从仓库根目录运行
# mvn -pl rabbitmq/lab-01-order-routing test

# 3. 观察：Management UI http://localhost:15672 （用户名/密码 lab / lab）

# 4. 清理（删除容器和数据卷）
docker compose down -v
```

端口 5672 或 15672 被占用时，修改 `docker-compose.yml` 的端口映射，并用 `-Drabbitmq.port=<端口>` 传给 Maven。

## 测试说明

| 测试类 | 用例数 | 验证内容 |
|---|---|---|
| `ExchangeRoutingTest` | 19 | 四类交换机的匹配与不匹配场景，断言实际收到消息的队列集合 |
| `AsyncDecouplingTest` | 2 | 同步 vs 异步耗时对照；故障注入证明“发布成功 ≠ 业务成功” |

## 目录结构

```
lab-01-order-routing/
├── docker-compose.yml
├── pom.xml
├── src/main/java/.../lab01
│   ├── config/    Topology（名称常量）、RabbitTopologyConfig（交换机/队列/绑定）、MessageConverterConfig
│   ├── model/     OrderEvent
│   ├── service/   OrderService、DownstreamSimulator（模拟下游延迟与故障）、ProcessingTracker
│   └── consumer/  DownstreamConsumers（通知、积分）
├── src/test/java/.../lab01
│   ├── ExchangeRoutingTest.java
│   └── AsyncDecouplingTest.java
└── docs/
    ├── experiment-report.md
    └── logs/
```
