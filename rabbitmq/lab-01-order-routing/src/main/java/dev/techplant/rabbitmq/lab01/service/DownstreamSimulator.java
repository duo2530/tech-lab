package dev.techplant.rabbitmq.lab01.service;

import dev.techplant.rabbitmq.lab01.model.OrderEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 模拟下游服务（通知、积分）的处理耗时与故障。同步调用与消费者共用，保证对照实验的条件一致。
 */
@Component
public class DownstreamSimulator {

    public static final String NOTIFY = "notify";
    public static final String POINTS = "points";

    private final long notifyDelayMs;
    private final long pointsDelayMs;

    public DownstreamSimulator(@Value("${lab.downstream.notify-delay-ms:300}") long notifyDelayMs,
                               @Value("${lab.downstream.points-delay-ms:500}") long pointsDelayMs) {
        this.notifyDelayMs = notifyDelayMs;
        this.pointsDelayMs = pointsDelayMs;
    }

    public void sendNotification(OrderEvent event) {
        process(NOTIFY, notifyDelayMs, event);
    }

    public void grantPoints(OrderEvent event) {
        process(POINTS, pointsDelayMs, event);
    }

    public long notifyDelayMs() {
        return notifyDelayMs;
    }

    public long pointsDelayMs() {
        return pointsDelayMs;
    }

    private void process(String service, long delayMs, OrderEvent event) {
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(service + " interrupted", e);
        }
        if (service.equals(event.faultTarget())) {
            throw new IllegalStateException("injected fault in " + service + " for order " + event.orderId());
        }
    }
}
