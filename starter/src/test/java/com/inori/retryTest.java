package com.inori;

import com.inori.constant.MQConstant;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Date;

/**
 * @author : KaelvihN
 * @date : 2023/9/30 18:50
 */

@SpringBootTest
@Slf4j
public class retryTest {

    @Test
    @SneakyThrows
    void retryProducer() {
        DefaultMQProducer producer = new DefaultMQProducer("retry_producer_group");
        producer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        producer.start();
        //设置重试次数
        producer.setRetryTimesWhenSendFailed(3);
        Message message = new Message("retryTopic", "RocketMQ,启动!!!".getBytes());
        //发送消息，设置超时时间为1ms
        producer.send(message, 1000);
        producer.shutdown();
    }

    @Test
    @SneakyThrows
    void retryConsumer() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("retry_consumer_group");
        consumer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        consumer.subscribe("retryTopic", "*");
        //设置重试次数(默认重试次数为16次)
        consumer.setMaxReconsumeTimes(2);
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            MessageExt messageExt = msgs.get(0);
            log.info(new Date() + ">>>>" +
                    new String(messageExt.getBody()) + ">>>>>" +
                    messageExt.getReconsumeTimes());
            //开始重试
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        });
        consumer.start();
        System.in.read();
    }

    @SneakyThrows
    @Test
    void retryDeadConsumer() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("retry_dead_consumer_group");
        consumer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        consumer.subscribe("%DLQ%retry_consumer_group", "*");
        consumer.registerMessageListener(((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                log.info(new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }));
        consumer.start();
        System.in.read();
    }

    @Test
    @SneakyThrows
    void retryAndHandleConsumer() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("retry_handle_consumer_group");
        consumer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        consumer.subscribe("retryTopic", "*");
        //设置重试次数(默认重试次数为16次)
        consumer.setMaxReconsumeTimes(3);
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            try {
                businessCode();
            } catch (Exception e) {
                MessageExt messageExt = msgs.get(0);
                if (messageExt.getReconsumeTimes() < consumer.getMaxReconsumeTimes()) {
                    log.info(new String(messageExt.getBody()) + ">>>" + messageExt.getReconsumeTimes());
                    //小于设置的最大重试次数，开始重试
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                } else {
                    log.info(new String(messageExt.getBody()) + "被记录到数据库,通知人工介入");
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            }
            //处理成功
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();
        System.in.read();
    }

    /**
     * 假设这是业务代码
     */
    void businessCode() {
        int error = 10 / 0;
    }


}
