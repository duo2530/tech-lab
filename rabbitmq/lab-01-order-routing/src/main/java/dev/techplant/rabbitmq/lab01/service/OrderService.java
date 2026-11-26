package dev.techplant.rabbitmq.lab01.service;

import dev.techplant.rabbitmq.lab01.config.Topology;
import dev.techplant.rabbitmq.lab01.model.OrderEvent;
import java.util.concurrent.TimeUnit;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

/**
 * 订单服务：同步调用下游 vs. 异步发布事件。
 */
@Service
public class OrderService {

    /**
     * 异步下单结果。
     *
     * @param startNanos           起始时刻
     * @param publishReturnedNanos 调用 send 到方法返回的耗时
     * @param confirmedNanos       调用 send 到收到 Broker 确认的耗时
     * @param brokerAcked          Broker 是否确认（只代表 Broker 收到，不代表消费或业务成功）
     */
    public record AsyncResult(long startNanos, long publishReturnedNanos, long confirmedNanos, boolean brokerAcked) {
    }

    private final RabbitTemplate rabbitTemplate;
    private final DownstreamSimulator downstream;

    public OrderService(RabbitTemplate rabbitTemplate, DownstreamSimulator downstream) {
        this.rabbitTemplate = rabbitTemplate;
        this.downstream = downstream;
    }

    /** 基线：订单服务同步串行调用通知与积分。 */
    public void placeOrderSync(OrderEvent event) {
        downstream.sendNotification(event);
        downstream.grantPoints(event);
    }

    /** 异步：只向 Fanout 交换机发布订单事件，下游各自消费。 */
    public AsyncResult placeOrderAsync(OrderEvent event) throws Exception {
        CorrelationData correlation = new CorrelationData(event.orderId());
        long start = System.nanoTime();
        rabbitTemplate.convertAndSend(Topology.ORDER_EVENTS_EX, "", event, correlation);
        long returned = System.nanoTime() - start;
        boolean acked = correlation.getFuture().get(5, TimeUnit.SECONDS).isAck();
        long confirmed = System.nanoTime() - start;
        return new AsyncResult(start, returned, confirmed, acked);
    }
}
