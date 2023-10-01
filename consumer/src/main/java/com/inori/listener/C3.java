package com.inori.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * @author : KaelvihN
 * @date : 2023/10/1 22:37
 */
@Component
@Slf4j
@RocketMQMessageListener(topic = "mode-topic",
        consumerGroup = "mode-consumer-group-a",
        messageModel = MessageModel.CLUSTERING)
public class C3 implements RocketMQListener<MessageExt> {
    @Override
    public void onMessage(MessageExt message) {
        log.info("a组第三个消费者:" + new String(message.getBody()));
    }
}
