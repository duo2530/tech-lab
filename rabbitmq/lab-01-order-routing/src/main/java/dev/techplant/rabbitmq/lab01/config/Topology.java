package dev.techplant.rabbitmq.lab01.config;

import java.util.List;

/**
 * 交换机、队列名称常量。路由实验与异步解耦实验使用各自独立的队列，互不干扰。
 */
public final class Topology {

    private Topology() {
    }

    // ---------- 路由实验（无消费者，测试直接从队列取消息断言） ----------
    public static final String DIRECT_EX = "lab01.route.direct";
    public static final String DIRECT_PAID_Q = "lab01.route.direct.paid";
    public static final String DIRECT_CANCELLED_Q = "lab01.route.direct.cancelled";
    public static final String KEY_PAID = "order.paid";
    public static final String KEY_CANCELLED = "order.cancelled";

    public static final String FANOUT_EX = "lab01.route.fanout";
    public static final String FANOUT_NOTIFY_Q = "lab01.route.fanout.notify";
    public static final String FANOUT_POINTS_Q = "lab01.route.fanout.points";
    public static final String FANOUT_AUDIT_Q = "lab01.route.fanout.audit";
    /** 声明了但没有绑定到 Fanout 交换机的队列，作为“不匹配”对照 */
    public static final String FANOUT_UNBOUND_Q = "lab01.route.fanout.unbound";

    public static final String TOPIC_EX = "lab01.route.topic";
    public static final String TOPIC_PAID_Q = "lab01.route.topic.paid";
    public static final String TOPIC_CN_Q = "lab01.route.topic.cn";
    public static final String TOPIC_AUDIT_Q = "lab01.route.topic.audit";
    public static final String TOPIC_PAID_PATTERN = "order.*.paid";
    public static final String TOPIC_CN_PATTERN = "order.cn.*";
    public static final String TOPIC_AUDIT_PATTERN = "order.#";

    public static final String HEADERS_EX = "lab01.route.headers";
    public static final String HEADERS_ALL_Q = "lab01.route.headers.all";
    public static final String HEADERS_ANY_Q = "lab01.route.headers.any";

    public static final List<String> DIRECT_QUEUES = List.of(DIRECT_PAID_Q, DIRECT_CANCELLED_Q);
    public static final List<String> FANOUT_QUEUES =
            List.of(FANOUT_NOTIFY_Q, FANOUT_POINTS_Q, FANOUT_AUDIT_Q, FANOUT_UNBOUND_Q);
    public static final List<String> TOPIC_QUEUES = List.of(TOPIC_PAID_Q, TOPIC_CN_Q, TOPIC_AUDIT_Q);
    public static final List<String> HEADERS_QUEUES = List.of(HEADERS_ALL_Q, HEADERS_ANY_Q);

    // ---------- 异步解耦实验（有消费者） ----------
    public static final String ORDER_EVENTS_EX = "lab01.async.order-events";
    public static final String ASYNC_NOTIFY_Q = "lab01.async.notify";
    public static final String ASYNC_POINTS_Q = "lab01.async.points";
}
