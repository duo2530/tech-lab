package dev.techplant.rabbitmq.lab01.config;

import static dev.techplant.rabbitmq.lab01.config.Topology.*;

import java.util.Map;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 声明四种交换机、队列与绑定。RabbitAdmin 会在连接建立时自动声明。
 */
@Configuration
public class RabbitTopologyConfig {

    @Bean
    public Declarables directTopology() {
        DirectExchange ex = new DirectExchange(DIRECT_EX, true, false);
        Queue paid = new Queue(DIRECT_PAID_Q, true);
        Queue cancelled = new Queue(DIRECT_CANCELLED_Q, true);
        return new Declarables(ex, paid, cancelled,
                BindingBuilder.bind(paid).to(ex).with(KEY_PAID),
                BindingBuilder.bind(cancelled).to(ex).with(KEY_CANCELLED));
    }

    @Bean
    public Declarables fanoutTopology() {
        FanoutExchange ex = new FanoutExchange(FANOUT_EX, true, false);
        Queue notify = new Queue(FANOUT_NOTIFY_Q, true);
        Queue points = new Queue(FANOUT_POINTS_Q, true);
        Queue audit = new Queue(FANOUT_AUDIT_Q, true);
        Queue unbound = new Queue(FANOUT_UNBOUND_Q, true);
        return new Declarables(ex, notify, points, audit, unbound,
                BindingBuilder.bind(notify).to(ex),
                BindingBuilder.bind(points).to(ex),
                BindingBuilder.bind(audit).to(ex));
    }

    @Bean
    public Declarables topicTopology() {
        TopicExchange ex = new TopicExchange(TOPIC_EX, true, false);
        Queue paid = new Queue(TOPIC_PAID_Q, true);
        Queue cn = new Queue(TOPIC_CN_Q, true);
        Queue audit = new Queue(TOPIC_AUDIT_Q, true);
        return new Declarables(ex, paid, cn, audit,
                BindingBuilder.bind(paid).to(ex).with(TOPIC_PAID_PATTERN),
                BindingBuilder.bind(cn).to(ex).with(TOPIC_CN_PATTERN),
                BindingBuilder.bind(audit).to(ex).with(TOPIC_AUDIT_PATTERN));
    }

    @Bean
    public Declarables headersTopology() {
        HeadersExchange ex = new HeadersExchange(HEADERS_EX, true, false);
        Queue all = new Queue(HEADERS_ALL_Q, true);
        Queue any = new Queue(HEADERS_ANY_Q, true);
        // x-match=all：所有头都必须匹配
        Binding allBinding = BindingBuilder.bind(all).to(ex)
                .whereAll(Map.<String, Object>of("vip", true, "channel", "app")).match();
        // x-match=any：任意一个头匹配即可
        Binding anyBinding = BindingBuilder.bind(any).to(ex)
                .whereAny(Map.<String, Object>of("region", "cn", "channel", "web")).match();
        return new Declarables(ex, all, any, allBinding, anyBinding);
    }

    @Bean
    public Declarables orderEventsTopology() {
        FanoutExchange ex = new FanoutExchange(ORDER_EVENTS_EX, true, false);
        Queue notify = new Queue(ASYNC_NOTIFY_Q, true);
        Queue points = new Queue(ASYNC_POINTS_Q, true);
        return new Declarables(ex, notify, points,
                BindingBuilder.bind(notify).to(ex),
                BindingBuilder.bind(points).to(ex));
    }
}
