package com.inori.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.annotation.SelectorType;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * @author : KaelvihN
 * @date : 2023/10/1 18:22
 */

@Component
@Slf4j
@RocketMQMessageListener(topic = "tag-key-topic",
        consumerGroup = "tag-key-group",
        selectorType = SelectorType.TAG,
        selectorExpression = "TagA||TagB")
public class TagKeyListener implements RocketMQListener<MessageExt> {

    @Override
    public void onMessage(MessageExt ext) {
        String keys = ext.getKeys();
        String body = new String(ext.getBody());
        log.info(keys + "=>" + body);
    }
}
