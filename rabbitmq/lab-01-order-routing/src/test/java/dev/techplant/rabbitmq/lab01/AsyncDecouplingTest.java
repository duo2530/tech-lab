package dev.techplant.rabbitmq.lab01;

import static dev.techplant.rabbitmq.lab01.config.Topology.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;

import dev.techplant.rabbitmq.lab01.model.OrderEvent;
import dev.techplant.rabbitmq.lab01.service.DownstreamSimulator;
import dev.techplant.rabbitmq.lab01.service.OrderService;
import dev.techplant.rabbitmq.lab01.service.OrderService.AsyncResult;
import dev.techplant.rabbitmq.lab01.service.ProcessingTracker;
import dev.techplant.rabbitmq.lab01.service.ProcessingTracker.Record;
import dev.techplant.rabbitmq.lab01.service.ProcessingTracker.Status;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 同步调用 vs. 异步发布的下游延迟对照，以及“发布成功 != 业务成功”的故障注入。
 * 需要先 docker compose up -d --wait。
 */
@SpringBootTest
class AsyncDecouplingTest {

    private static final Logger log = LoggerFactory.getLogger(AsyncDecouplingTest.class);

    @Autowired
    OrderService orderService;
    @Autowired
    ProcessingTracker tracker;
    @Autowired
    DownstreamSimulator downstream;
    @Autowired
    RabbitAdmin rabbitAdmin;

    @BeforeEach
    void purge() {
        rabbitAdmin.purgeQueue(ASYNC_NOTIFY_Q);
        rabbitAdmin.purgeQueue(ASYNC_POINTS_Q);
    }

    @Test
    void asyncPublishReturnsBeforeDownstreamFinishes() throws Exception {
        long syncSerialMs = downstream.notifyDelayMs() + downstream.pointsDelayMs();
        long slowestMs = Math.max(downstream.notifyDelayMs(), downstream.pointsDelayMs());

        // 基线：同步串行调用，下单耗时约等于各下游耗时之和
        OrderEvent syncOrder = OrderEvent.of("SYNC-" + UUID.randomUUID(), "created", "cn");
        long syncStart = System.nanoTime();
        orderService.placeOrderSync(syncOrder);
        long syncMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - syncStart);

        // 异步：发布事件即返回
        OrderEvent order = OrderEvent.of("ASYNC-" + UUID.randomUUID(), "created", "cn");
        AsyncResult result = orderService.placeOrderAsync(order);
        awaitRecords(order.orderId(), 2);
        List<Record> records = tracker.get(order.orderId());
        long lastDoneNanos = records.stream().mapToLong(Record::completedAtNanos).max().orElseThrow();
        long consumeDoneMs = TimeUnit.NANOSECONDS.toMillis(lastDoneNanos - result.startNanos());
        long publishReturnMs = TimeUnit.NANOSECONDS.toMillis(result.publishReturnedNanos());
        long confirmMs = TimeUnit.NANOSECONDS.toMillis(result.confirmedNanos());
        long gapMs = consumeDoneMs - publishReturnMs;

        log.info("[timing] downstream delays: notify={}ms points={}ms", downstream.notifyDelayMs(),
                downstream.pointsDelayMs());
        log.info("[timing] sync order total          = {} ms", syncMs);
        log.info("[timing] async publish returned    = {} ms", publishReturnMs);
        log.info("[timing] async broker confirmed    = {} ms", confirmMs);
        log.info("[timing] async consumers all done  = {} ms (after publish start)", consumeDoneMs);
        log.info("[timing] gap (consume done - publish returned) = {} ms", gapMs);

        assertThat(result.brokerAcked()).isTrue();
        assertThat(syncMs).isGreaterThanOrEqualTo(syncSerialMs);
        // 发布耗时应远小于同步总耗时，这里取一半作为宽松阈值，避免机器抖动导致误报
        assertThat(publishReturnMs).isLessThan(syncMs / 2);
        // 两个下游并行消费，完成时间约等于最慢的下游，而不是两者之和
        assertThat(consumeDoneMs).isGreaterThanOrEqualTo(slowestMs).isLessThan(syncSerialMs + 500);
        // 异步方式下，发布返回与消费完成之间存在明显时间差
        assertThat(gapMs).isGreaterThan(slowestMs / 2);
        assertThat(records).extracting(Record::status).containsOnly(Status.SUCCESS);
    }

    @Test
    void publishAckedDoesNotMeanBusinessSucceeded() throws Exception {
        OrderEvent order = OrderEvent.of("FAULT-" + UUID.randomUUID(), "created", "cn")
                .withFault(DownstreamSimulator.POINTS);

        AsyncResult[] holder = new AsyncResult[1];
        // 下单调用不抛异常，Broker 也确认了
        assertThatCode(() -> holder[0] = orderService.placeOrderAsync(order)).doesNotThrowAnyException();
        assertThat(holder[0].brokerAcked()).isTrue();

        awaitRecords(order.orderId(), 2);
        List<Record> records = tracker.get(order.orderId());
        log.info("[fault] publish returned and broker acked = true, downstream records = {}", records);

        // 但积分下游实际处理失败：发布成功/Broker 确认 != 业务成功
        assertThat(records).filteredOn(r -> r.service().equals(DownstreamSimulator.NOTIFY))
                .extracting(Record::status).containsExactly(Status.SUCCESS);
        assertThat(records).filteredOn(r -> r.service().equals(DownstreamSimulator.POINTS))
                .extracting(Record::status).containsExactly(Status.FAILED);
    }

    private void awaitRecords(String orderId, int expected) {
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20))
                .until(() -> tracker.get(orderId).size() >= expected);
    }
}
