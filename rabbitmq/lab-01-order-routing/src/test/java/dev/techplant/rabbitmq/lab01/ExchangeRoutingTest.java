package dev.techplant.rabbitmq.lab01;

import static dev.techplant.rabbitmq.lab01.config.Topology.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 验证 Direct / Fanout / Topic / Headers 四种交换机的匹配与不匹配场景。
 * 路由实验的队列没有消费者，测试发布后直接从各队列取消息，断言“实际收到消息的队列集合”。
 * 需要先 docker compose up -d --wait。
 */
@SpringBootTest
class ExchangeRoutingTest {

    private static final Logger log = LoggerFactory.getLogger(ExchangeRoutingTest.class);

    @Autowired
    RabbitTemplate rabbitTemplate;
    @Autowired
    RabbitAdmin rabbitAdmin;

    @BeforeEach
    void purge() {
        Stream.of(DIRECT_QUEUES, FANOUT_QUEUES, TOPIC_QUEUES, HEADERS_QUEUES)
                .flatMap(List::stream)
                .forEach(rabbitAdmin::purgeQueue);
    }

    // ---------------- Direct ----------------

    static Stream<Arguments> directCases() {
        return Stream.of(
                Arguments.of(KEY_PAID, Set.of(DIRECT_PAID_Q)),
                Arguments.of(KEY_CANCELLED, Set.of(DIRECT_CANCELLED_Q)),
                // 不匹配：key 必须完全相等
                Arguments.of("order.paid.extra", Set.of()),
                Arguments.of("ORDER.PAID", Set.of()));
    }

    @ParameterizedTest(name = "direct key={0} -> {1}")
    @MethodSource("directCases")
    void direct(String routingKey, Set<String> expected) {
        publish(DIRECT_EX, routingKey, new MessageProperties());
        assertReceived("direct", routingKey, DIRECT_QUEUES, expected);
    }

    // ---------------- Fanout ----------------

    @ParameterizedTest(name = "fanout key=''{0}'' -> 全部已绑定队列")
    @MethodSource("fanoutKeys")
    void fanout(String routingKey) {
        publish(FANOUT_EX, routingKey, new MessageProperties());
        // 匹配：忽略 routing key，所有已绑定队列都收到；不匹配：未绑定的队列收不到
        assertReceived("fanout", routingKey, FANOUT_QUEUES,
                Set.of(FANOUT_NOTIFY_Q, FANOUT_POINTS_Q, FANOUT_AUDIT_Q));
    }

    static Stream<String> fanoutKeys() {
        return Stream.of("", "order.paid", "anything.at.all");
    }

    // ---------------- Topic ----------------

    static Stream<Arguments> topicCases() {
        return Stream.of(
                // * 匹配恰好一个单词；# 匹配零个或多个单词
                Arguments.of("order.cn.paid", Set.of(TOPIC_PAID_Q, TOPIC_CN_Q, TOPIC_AUDIT_Q)),
                Arguments.of("order.us.paid", Set.of(TOPIC_PAID_Q, TOPIC_AUDIT_Q)),
                Arguments.of("order.cn.created", Set.of(TOPIC_CN_Q, TOPIC_AUDIT_Q)),
                Arguments.of("order.cn.paid.refund", Set.of(TOPIC_AUDIT_Q)),
                Arguments.of("order", Set.of(TOPIC_AUDIT_Q)),
                // 不匹配：前缀不是 order
                Arguments.of("payment.cn.paid", Set.of()));
    }

    @ParameterizedTest(name = "topic key={0} -> {1}")
    @MethodSource("topicCases")
    void topic(String routingKey, Set<String> expected) {
        publish(TOPIC_EX, routingKey, new MessageProperties());
        assertReceived("topic", routingKey, TOPIC_QUEUES, expected);
    }

    // ---------------- Headers ----------------

    static Stream<Arguments> headersCases() {
        return Stream.of(
                // all 队列：vip=true 且 channel=app；any 队列：region=cn 或 channel=web
                Arguments.of(Map.of("vip", true, "channel", "app"), Set.of(HEADERS_ALL_Q)),
                Arguments.of(Map.of("vip", true, "channel", "app", "region", "cn"),
                        Set.of(HEADERS_ALL_Q, HEADERS_ANY_Q)),
                Arguments.of(Map.of("channel", "web"), Set.of(HEADERS_ANY_Q)),
                Arguments.of(Map.of("region", "cn"), Set.of(HEADERS_ANY_Q)),
                // 不匹配：all 只满足一半；any 一个都不满足
                Arguments.of(Map.of("vip", true), Set.of()),
                Arguments.of(Map.of("vip", false, "channel", "app", "region", "us"), Set.of()));
    }

    @ParameterizedTest(name = "headers {0} -> {1}")
    @MethodSource("headersCases")
    void headers(Map<String, Object> headers, Set<String> expected) {
        MessageProperties props = new MessageProperties();
        headers.forEach(props::setHeader);
        // routing key 对 Headers 交换机无效，这里故意给一个看似能匹配其他类型的 key
        publish(HEADERS_EX, KEY_PAID, props);
        assertReceived("headers", headers.toString(), HEADERS_QUEUES, expected);
    }

    // ---------------- helpers ----------------

    /** 发布并等待 Broker 确认，确认后消息已被路由并写入目标队列，之后用 basicGet 取消息是确定的。 */
    private void publish(String exchange, String routingKey, MessageProperties props) {
        Message message = MessageBuilder.withBody("order-event".getBytes())
                .andProperties(props).build();
        rabbitTemplate.invoke(ops -> {
            ops.send(exchange, routingKey, message);
            ops.waitForConfirmsOrDie(5000);
            return null;
        });
    }

    private void assertReceived(String type, String detail, List<String> queues, Set<String> expected) {
        Set<String> actual = new LinkedHashSet<>();
        for (String queue : queues) {
            if (rabbitTemplate.receive(queue) != null) {
                actual.add(queue);
            }
        }
        log.info("[routing] type={} detail={} expected={} actual={}", type, detail, expected, actual);
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }
}
