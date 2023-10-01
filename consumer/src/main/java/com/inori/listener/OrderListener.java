package com.inori.listener;

import com.inori.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * @author : KaelvihN
 * @date : 2023/10/1 18:15
 */
@Component
@Slf4j
@RocketMQMessageListener(topic = "order-topic",
        consumerGroup = "order-group",
        //默认为CONCURRENTLY模式
        consumeMode = ConsumeMode.ORDERLY)
public class OrderListener implements RocketMQListener<MessageExt> {
    /**
     * 消费者方法(顺序)
     * @param ext
     */
    @Override
    public void onMessage(MessageExt ext) {
        String message = new String(ext.getBody());
        log.info(message);
    }
}
