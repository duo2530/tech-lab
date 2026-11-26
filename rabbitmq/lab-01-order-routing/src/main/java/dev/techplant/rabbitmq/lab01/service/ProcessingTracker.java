package dev.techplant.rabbitmq.lab01.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.springframework.stereotype.Component;

/**
 * 记录各下游对每个订单的处理结果与完成时间（System.nanoTime），用于测量异步链路的真实完成时刻。
 */
@Component
public class ProcessingTracker {

    public enum Status { SUCCESS, FAILED }

    public record Record(String service, Status status, long completedAtNanos) {
    }

    private final Map<String, List<Record>> records = new ConcurrentHashMap<>();

    public void record(String orderId, String service, Status status) {
        records.computeIfAbsent(orderId, k -> new CopyOnWriteArrayList<>())
                .add(new Record(service, status, System.nanoTime()));
    }

    public List<Record> get(String orderId) {
        return records.getOrDefault(orderId, List.of());
    }
}
