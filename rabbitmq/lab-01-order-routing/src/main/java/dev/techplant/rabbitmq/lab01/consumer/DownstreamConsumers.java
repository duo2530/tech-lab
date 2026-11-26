package dev.techplant.rabbitmq.lab01.consumer;

import dev.techplant.rabbitmq.lab01.config.Topology;
import dev.techplant.rabbitmq.lab01.model.OrderEvent;
import dev.techplant.rabbitmq.lab01.service.DownstreamSimulator;
import dev.techplant.rabbitmq.lab01.service.ProcessingTracker;
import dev.techplant.rabbitmq.lab01.service.ProcessingTracker.Status;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * 通知、积分两个下游消费者，各自独立队列，互不阻塞。
 */
@Component
public class DownstreamConsumers {

    private static final Logger log = LoggerFactory.getLogger(DownstreamConsumers.class);

    private final DownstreamSimulator downstream;
    private final ProcessingTracker tracker;

    public DownstreamConsumers(DownstreamSimulator downstream, ProcessingTracker tracker) {
        this.downstream = downstream;
        this.tracker = tracker;
    }

    @RabbitListener(queues = Topology.ASYNC_NOTIFY_Q)
    public void onNotify(OrderEvent event) {
        handle(DownstreamSimulator.NOTIFY, event, downstream::sendNotification);
    }

    @RabbitListener(queues = Topology.ASYNC_POINTS_Q)
    public void onPoints(OrderEvent event) {
        handle(DownstreamSimulator.POINTS, event, downstream::grantPoints);
    }

    private void handle(String service, OrderEvent event, Consumer<OrderEvent> action) {
        try {
            action.accept(event);
            tracker.record(event.orderId(), service, Status.SUCCESS);
            log.info("[{}] order {} processed", service, event.orderId());
        } catch (RuntimeException e) {
            tracker.record(event.orderId(), service, Status.FAILED);
            log.warn("[{}] order {} FAILED: {}", service, event.orderId(), e.getMessage());
            // 不重新入队，避免无限重试；死信与重试策略留给后续实验
            throw new AmqpRejectAndDontRequeueException(e.getMessage(), e);
        }
    }
}
