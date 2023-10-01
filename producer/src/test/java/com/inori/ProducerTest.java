package com.inori;


import com.alibaba.fastjson.JSON;
import com.inori.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;


@SpringBootTest
@Slf4j
public class ProducerTest {
    @Resource
    private RocketMQTemplate template;

    List<Order> msgs = Arrays.asList(
            new Order(1, "KaelvihN", "下单"),
            new Order(1, "KaelvihN", "付款"),
            new Order(1, "KaelvihN", "发货"),
            new Order(2, "tom", "下单"),
            new Order(2, "tom", "付款"),
            new Order(2, "tom", "发货")
    );

    @Test
    void simpleSend() {
        //同步消息
        template.syncSend("boot-topic", "同步消息".getBytes());
        //异步消息
        template.asyncSend("boot-topic", "异步消息".getBytes(), new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                log.info("SUCCESS");
            }

            @Override
            public void onException(Throwable throwable) {
                log.error("ERROR");
            }
        });
        //单向消息
        template.sendOneWay("boot-topic", "单向消息");
        //延迟消息
        Message<String> delayMessage = MessageBuilder.withPayload("延迟消息").build();
        template.syncSend("boot-topic", delayMessage, 10000, 3);
        //顺序消息
        msgs.forEach(msgModel -> {
            template.syncSendOrderly("order-topic",
                    JSON.toJSONString(msgModel),
                    msgModel.getId().toString());
        });
        /**
         * 消息加key和tag
         * destination = topic + : +tag
         */
        Message<String> signMessageA = MessageBuilder.withPayload("key和tag的消息")
                .setHeader(RocketMQHeaders.KEYS, "A")
                .build();
        Message<String> signMessageB = MessageBuilder.withPayload("key和tag的消息")
                .setHeader(RocketMQHeaders.KEYS, "B")
                .build();
        Message<String> signMessageC = MessageBuilder.withPayload("key和tag的消息")
                .setHeader(RocketMQHeaders.KEYS, "C")
                .build();

        template.syncSend("tag-key-topic:TagA", signMessageA);
        template.syncSend("tag-key-topic:TagB", signMessageB);
        template.syncSend("tag-key-topic:TagC", signMessageC);
    }

    @Test
    void consumerMode() {
        for (int i = 0; i < 5; i++) {
            template.syncSend("mode-topic-b", "message" + i);
        }
    }

}