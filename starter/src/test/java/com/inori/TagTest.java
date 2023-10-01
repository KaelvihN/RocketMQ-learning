package com.inori;

import com.inori.constant.MQConstant;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author : KaelvihN
 * @date : 2023/9/29 23:51
 */
@SpringBootTest
@Slf4j
public class TagTest {

    @SneakyThrows
    @Test
    void tagProducer() {
        DefaultMQProducer producer = new DefaultMQProducer("tag_producer_group");
        producer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        producer.start();
        Message messageA = new Message("tagTopic", "tagA", "tagA消息".getBytes());
        producer.send(messageA);
        Message messageB = new Message("tagTopic", "tagB", "tagB消息".getBytes());
        producer.send(messageB);
        Message messageC = new Message("tagTopic", "tagC", "tagC消息".getBytes());
        producer.send(messageC);
        producer.shutdown();
    }

    @SneakyThrows
    @Test
    void tagConsumer() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tag_consumer_group");
        consumer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        consumer.subscribe("tagTopic", "tagA||tagB");
        consumer.registerMessageListener(((MessageListenerConcurrently) (msgs, context) -> {
            log.info(new String(msgs.get(0).getBody()));
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }));
        consumer.start();
        System.in.read();
    }
}
