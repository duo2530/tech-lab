package dev.techplant.rabbitmq.lab01.model;

import java.math.BigDecimal;

/**
 * 订单事件。
 *
 * @param orderId     订单号
 * @param eventType   事件类型，如 created / paid / cancelled
 * @param region      区域，如 cn / us
 * @param amount      金额
 * @param faultTarget 故障注入目标下游（如 points），为 null 表示不注入
 */
public record OrderEvent(String orderId, String eventType, String region, BigDecimal amount, String faultTarget) {

    public static OrderEvent of(String orderId, String eventType, String region) {
        return new OrderEvent(orderId, eventType, region, new BigDecimal("99.00"), null);
    }

    public OrderEvent withFault(String target) {
        return new OrderEvent(orderId, eventType, region, amount, target);
    }
}
