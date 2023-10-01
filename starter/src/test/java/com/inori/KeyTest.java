package com.inori;

import com.inori.constant.MQConstant;
import com.inori.modle.Order;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;

/**
 * @author : KaelvihN
 * @date : 2023/9/30 2:04
 */
@SpringBootTest
@Slf4j
public class KeyTest {
    @Resource
    private RedisTemplate<String, String> template;

    List<Order> orderList = Arrays.asList(
            new Order(1, "KaelvihN", "下单"),
            new Order(1, "KaelvihN", "付款"),
            new Order(1, "KaelvihN", "发货"),
            new Order(2, "Tom", "下单"),
            new Order(2, "Tom", "付款"),
            new Order(2, "Tom", "发货"),
            new Order(2, "Tom", "付款"),
            new Order(2, "Tom", "发货")
    );

    @SneakyThrows
    @Test
    void keyProducer() {
        DefaultMQProducer producer = new DefaultMQProducer("key_producer_group");
        producer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        producer.start();
        for (Order order : orderList) {
            String key = order.getId() + "_" + order.getState();
            Message message = new Message("keyTopic", null, key, order.toString().getBytes());
            producer.send(message,((mqs, msg, arg) -> {
                //通过对id取模使得相同id的订单消息处于同一个队列
                int queueSize = mqs.size();
                Integer id = (Integer) arg;
                int index = id % queueSize;
                return mqs.get(index);
            }),order.getId());
        }
        producer.shutdown();
    }

    @SneakyThrows
    @Test
    void keyConsumer() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("key_consumer_group");
        consumer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        consumer.subscribe("keyTopic", "*");
        consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {
            SetOperations<String, String> setOps = template.opsForSet();
            for (MessageExt msg : msgs) {
                //判断key是否在redis中
                if (!setOps.isMember("messageId", msg.getKeys())) {
                    setOps.add("messageId", msg.getKeys());
                    log.info(new String(msg.getBody()));
                } else {
                    log.warn(new String(msg.getBody()) + "是重复消息");
                }
            }
            return ConsumeOrderlyStatus.SUCCESS;
        });
        consumer.start();
        System.in.read();
    }

}
