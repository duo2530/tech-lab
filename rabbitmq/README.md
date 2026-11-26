# RabbitMQ 技术实验路线

## 目标

通过可运行、可复现的工程实验掌握 RabbitMQ，而不是单纯背诵面试题。每个实验形成 GitHub 代码、实验报告，并可独立制作 B 站技术视频。实验经验与真实生产经验应明确区分。

## 能力清单

| 模块 | 知识点 |
|---|---|
| 基础架构 | Producer、Consumer、Broker、Exchange、Queue、Binding、Virtual Host |
| 协议 | AMQP 0-9-1、Connection、Channel、Heartbeat |
| 路由 | Direct、Fanout、Topic、Headers、Alternate Exchange |
| 可靠投递 | Publisher Confirm、mandatory/Return、持久化、手动 ACK、本地消息表 |
| 消费 | 幂等、重复消费、顺序、并发、Prefetch |
| 异常处理 | 重试、DLX、TTL、延迟消息、毒性消息、补偿 |
| 高可用 | 集群、Quorum Queue、Raft、Leader 选举、故障转移 |
| 性能 | 异步确认、批量确认、消费者并发、流量控制、吞吐量 |
| 可观测性 | Management UI、Prometheus、Grafana、内存与磁盘告警 |
| 选型 | Classic Queue、Quorum Queue、Stream，与 Kafka、RocketMQ 的场景比较 |

## 实验 Backlog

以下均为待实现计划，不代表已经完成。

| 状态 | 编号 | 实验 | 验收产物 |
|---|---|---|---|
| 已完成（待 PR 审核） | 01 | 订单异步处理与路由模型 | [lab-01-order-routing](lab-01-order-routing/README.md)；[实验报告](lab-01-order-routing/docs/experiment-report.md) |
| 待做 | 02 | 秒杀订单削峰 | 对比直接处理与排队处理的吞吐、延迟和积压曲线 |
| 待做 | 03 | 生产者发送时断网、路由失败 | 注入连接中断与不可路由消息，验证 Confirm、Return、重发策略及重复风险 |
| 待做 | 04 | 消费者处理过程中宕机 | 验证 ACK 前后宕机的重投递差异 |
| 待做 | 05 | 支付消息重复投递 | 基于消息 ID 和数据库唯一约束实现幂等，验证重复投递不重复扣款 |
| 待做 | 06 | 订单状态消息乱序 | 复现并发消费乱序，验证单活消费者或业务版本号方案及吞吐代价 |
| 待做 | 07 | 订单超时取消 | 验证 TTL + DLX 延迟处理、误差及队头阻塞限制 |
| 待做 | 08 | 下游服务持续异常 | 有限次数重试、退避、死信隔离、人工重放；避免无限 requeue |
| 待做 | 09 | 数据库提交后消息发送失败 | 实现 Transactional Outbox，验证补发与消费幂等 |
| 待做 | 10 | 百万条消息积压 | 逐级增加负载，记录积压原因、恢复速度、资源限制及操作手册 |
| 待做 | 11 | 集群节点宕机 | 三节点 Quorum Queue，验证 Leader 故障与多数派丢失时的行为 |
| 待做 | 12 | 参数调优与压测 | 使用 PerfTest，比较不同消息大小、Prefetch、并发与确认策略的吞吐及延迟 |
| 待做 | 13 | 内存、磁盘告警与流控 | 在隔离环境触发资源告警，观察生产者阻塞及恢复 |
| 待做 | 14 | 监控面板 | Prometheus + Grafana 展示发布/消费速率、Ready、Unacked、资源及告警 |

## 实验规范

建议使用 Java、Spring Boot、Spring AMQP、Docker Compose，固定依赖和镜像版本；按现有仓库 Maven 结构组织，实际实现时再确认父 POM 和模块关系。优先使用 RabbitMQ 4.x，具体版本在实现阶段选定。

每个实验应包含：
- 问题描述、假设、架构图或时序图。
- 可复现环境、生产者/消费者代码、自动化测试。
- 故障注入或压测脚本、日志与观察指标。
- 对照实验、实际结果、限制、结论及清理方式。
- README 中提供运行命令；压测只在隔离环境进行。

建议目录：rabbitmq/lab-03-reliable-delivery/，其中包含 README.md、pom.xml、src/、scripts/、docker-compose.yml、docs/，按实际需要调整。

## 关键边界

- Publisher Confirm 不代表消费成功；不可路由的消息也可能获得确认，应结合 mandatory/Return 判断。
- 持久化与确认并不能消除重复投递；业务端仍需幂等。
- FIFO 不等同于并发消费后的业务处理顺序。
- RabbitMQ 4.x 不再提供经典镜像队列，应重点研究 Quorum Queue 的多数派机制。
- Outbox 通常实现至少一次投递与最终一致性，不应宣称端到端 exactly-once。

## 官方资料

- [Reliability Guide](https://www.rabbitmq.com/docs/reliability)
- [Publisher Confirms and Consumer Acknowledgements](https://www.rabbitmq.com/docs/confirms)
- [Queues](https://www.rabbitmq.com/docs/queues)
- [Quorum Queues](https://www.rabbitmq.com/docs/quorum-queues)
- [Clustering](https://www.rabbitmq.com/docs/clustering)
- [Flow Control](https://www.rabbitmq.com/docs/flow-control)
- [Monitoring](https://www.rabbitmq.com/docs/monitoring)
- [PerfTest](https://rabbitmq.github.io/rabbitmq-perf-test/)

## 简历表达（完成相应实验后据实使用）

熟练掌握 RabbitMQ 消息中间件，理解 AMQP 协议、消息路由、发布确认与消费确认机制；能够设计可靠投递、消费幂等、重试死信及最终一致性方案，并通过故障注入与压力测试验证消息积压、高可用和性能优化方案。

只有具备相应真实经验时才描述生产环境故障排查及调优经历。

## 下一步

优先完成 Lab 01，建立可运行的最小工程及 Docker Compose 环境，再推进 Lab 03–05 的可靠性实验。每完成一项，更新状态并链接实验报告与视频（如已发布）。
