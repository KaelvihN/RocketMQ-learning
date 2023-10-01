package com.inori.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;


/**
 * @author : KaelvihN
 * @date : 2023/10/1 16:55
 */
@RocketMQMessageListener(topic = "boot-topic", consumerGroup = "boot-consumer")
@Component
@Slf4j
public class BootListener implements RocketMQListener<MessageExt> {
    /**
     * 消费者方法(同步，异步，单向，延迟)
     * @param ext
     */
    @Override
    public void onMessage(MessageExt ext) {
        String msgBody = new String(ext.getBody());
        log.info(msgBody);
    }
}
