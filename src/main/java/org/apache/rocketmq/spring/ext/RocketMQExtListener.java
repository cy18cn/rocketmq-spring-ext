package org.apache.rocketmq.spring.ext;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.ext.store.MessageEntity;
import org.apache.rocketmq.spring.ext.store.MessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class RocketMQExtListener implements RocketMQListener<MessageExt> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RocketMQExtListener.class);

    private MessageStore messageStore;

    @Override
    public void onMessage(MessageExt messageExt) {
        LOGGER.info("({}) received msgId={}, reties={}, content={} ",
                this.getClass().getTypeName(),
                messageExt.getMsgId(),
                messageExt.getReconsumeTimes(),
                new String(messageExt.getBody()));
        consume(messageExt);
        this.messageStore.update(messageExt.getMsgId(), MessageEntity.MessageStatus.CONSUMED);
        LOGGER.info("Complete to consume msg");
    }

    abstract void consume(MessageExt messageExt);
}
