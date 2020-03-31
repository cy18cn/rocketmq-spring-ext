package org.apache.rocketmq.spring.ext;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.ext.store.MessageEntity;
import org.apache.rocketmq.spring.ext.store.MessageStore;
import org.apache.rocketmq.spring.support.RocketMQUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

public class RocketMQExtTemplate extends RocketMQTemplate {
    private static final Logger LOGGER = LoggerFactory.getLogger(RocketMQExtTemplate.class);

    @Getter
    @Setter
    private MessageStore messageStore;

    /**
     * syncSend batch messages in a given timeout.
     *
     * @param destination formats: `topicName:tags`
     * @param messages Collection of {@link org.springframework.messaging.Message}
     * @param timeout send timeout with millis
     * @return {@link SendResult}
     */
    @Override
    public <T extends Message> SendResult syncSend(String destination, Collection<T> messages, long timeout) {
        if (Objects.isNull(messages) || messages.size() == 0) {
            LOGGER.error("syncSend with batch failed. destination:{}, messages is empty ", destination);
            throw new IllegalArgumentException("`messages` can not be empty");
        }

        try {
            long now = System.currentTimeMillis();
            Collection<org.apache.rocketmq.common.message.Message> rmqMsgs = new ArrayList<>();
            for (Message msg : messages) {
                if (Objects.isNull(msg) || Objects.isNull(msg.getPayload())) {
                    LOGGER.warn("Found a message empty in the batch, skip it");
                    continue;
                }
                rmqMsgs.add(this.createRocketMqMessage(destination, msg));
            }

            SendResult sendResult = getProducer().send(rmqMsgs, timeout);
            // 同步发送消息，只要不抛异常就是成功
            this.persistMessage(rmqMsgs, true);
            long costTime = System.currentTimeMillis() - now;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("send messages cost: {} ms, msgId:{}", costTime, sendResult.getMsgId());
            }
            return sendResult;
        } catch (Exception e) {
            LOGGER.error("syncSend with batch failed. destination:{}, messages.size:{} ", destination, messages.size());
            throw new MessagingException(e.getMessage(), e);
        }
    }

    /**
     * Same to {@link #syncSend(String, Message)} with send timeout specified in addition.
     *
     * @param destination formats: `topicName:tags`
     * @param message {@link org.springframework.messaging.Message}
     * @param timeout send timeout with millis
     * @param delayLevel level for the delay message
     * @return {@link SendResult}
     */
    @Override
    public SendResult syncSend(String destination, Message<?> message, long timeout, int delayLevel) {
        if (Objects.isNull(message) || Objects.isNull(message.getPayload())) {
            LOGGER.error("syncSend failed. destination:{}, message is null ", destination);
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }
        try {
            long now = System.currentTimeMillis();
            org.apache.rocketmq.common.message.Message rocketMsg = this.createRocketMqMessage(destination, message);
            if (delayLevel > 0) {
                rocketMsg.setDelayTimeLevel(delayLevel);
            }
            SendResult sendResult = getProducer().send(rocketMsg, timeout);
            // 同步发送消息，只要不抛异常就是成功
            this.persistMessage(rocketMsg, true);
            long costTime = System.currentTimeMillis() - now;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("send message cost: {} ms, msgId:{}", costTime, sendResult.getMsgId());
            }
            return sendResult;
        } catch (Exception e) {
            LOGGER.error("syncSend failed. destination:{}, message:{} ", destination, message);
            throw new MessagingException(e.getMessage(), e);
        }
    }

    /**
     * Same to {@link #syncSendOrderly(String, Message, String)} with send timeout specified in addition.
     *
     * @param destination formats: `topicName:tags`
     * @param message {@link org.springframework.messaging.Message}
     * @param hashKey use this key to select queue. for example: orderId, productId ...
     * @param timeout send timeout with millis
     * @return {@link SendResult}
     */
    @Override
    public SendResult syncSendOrderly(String destination, Message<?> message, String hashKey, long timeout) {
        if (Objects.isNull(message) || Objects.isNull(message.getPayload())) {
            LOGGER.error("syncSendOrderly failed. destination:{}, message is null ", destination);
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }
        try {
            long now = System.currentTimeMillis();
            org.apache.rocketmq.common.message.Message rocketMsg = this.createRocketMqMessage(destination, message);
            SendResult sendResult = getProducer().send(rocketMsg, getMessageQueueSelector(), hashKey, timeout);
            this.persistMessage(rocketMsg, true);
            long costTime = System.currentTimeMillis() - now;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("send message cost: {} ms, msgId:{}", costTime, sendResult.getMsgId());
            }
            return sendResult;
        } catch (Exception e) {
            LOGGER.error("syncSendOrderly failed. destination:{}, message:{} ", destination, message);
            throw new MessagingException(e.getMessage(), e);
        }
    }

    /**
     * Same to {@link #asyncSend(String, Message, SendCallback)} with send timeout and delay level specified in
     * addition.
     *
     * @param destination formats: `topicName:tags`
     * @param message {@link org.springframework.messaging.Message}
     * @param sendCallback {@link SendCallback}
     * @param timeout send timeout with millis
     * @param delayLevel level for the delay message
     */
    @Override
    public void asyncSend(String destination, Message<?> message, SendCallback sendCallback, long timeout,
                          int delayLevel) {
        if (Objects.isNull(message) || Objects.isNull(message.getPayload())) {
            LOGGER.error("asyncSend failed. destination:{}, message is null ", destination);
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }
        try {
            org.apache.rocketmq.common.message.Message rocketMsg = this.createRocketMqMessage(destination, message);
            if (delayLevel > 0) {
                rocketMsg.setDelayTimeLevel(delayLevel);
            }
            getProducer().send(rocketMsg, sendCallback, timeout);
            this.persistMessage(rocketMsg, false);
        } catch (Exception e) {
            LOGGER.info("asyncSend failed. destination:{}, message:{} ", destination, message);
            throw new MessagingException(e.getMessage(), e);
        }
    }

    /**
     * Same to {@link #asyncSendOrderly(String, Message, String, SendCallback)} with send timeout specified in
     * addition.
     *
     * @param destination formats: `topicName:tags`
     * @param message {@link org.springframework.messaging.Message}
     * @param hashKey use this key to select queue. for example: orderId, productId ...
     * @param sendCallback {@link SendCallback}
     * @param timeout send timeout with millis
     */
    @Override
    public void asyncSendOrderly(String destination, Message<?> message, String hashKey, SendCallback sendCallback,
                                 long timeout) {
        if (Objects.isNull(message) || Objects.isNull(message.getPayload())) {
            LOGGER.error("asyncSendOrderly failed. destination:{}, message is null ", destination);
            throw new IllegalArgumentException("`message` and `message.payload` cannot be null");
        }
        try {
            org.apache.rocketmq.common.message.Message rocketMsg = this.createRocketMqMessage(destination, message);
            getProducer().send(rocketMsg, getMessageQueueSelector(), hashKey, sendCallback, timeout);
            this.persistMessage(rocketMsg, false);
        } catch (Exception e) {
            LOGGER.error("asyncSendOrderly failed. destination:{}, message:{} ", destination, message);
            throw new MessagingException(e.getMessage(), e);
        }
    }

    private void persistMessage(Collection<org.apache.rocketmq.common.message.Message> messages, boolean sent) {
        if (messageStore == null) {
            return;
        }

        if (Objects.isNull(messages) && messages.size() == 0) {
            return;
        }

        for (org.apache.rocketmq.common.message.Message message : messages) {
            this.persistMessage(message, sent);
        }
    }

    private void persistMessage(org.apache.rocketmq.common.message.Message message, boolean sent) {
        if (messageStore == null) {
            return;
        }

        String msgId = MessageClientIDSetter.getUniqID(message);
        MessageEntity messageEntity = new MessageEntity();
        messageEntity.setMsgId(msgId);
        messageEntity.setHeaders(JSON.toJSONString(message.getProperties()));
        messageEntity.setContent(new String(message.getBody()));
        messageEntity.setStatus(sent ?
                MessageEntity.MessageStatus.SENT.getVal() :
                MessageEntity.MessageStatus.SENDING.getVal());
        messageEntity.setTopic(message.getTopic());
        messageEntity.setTags(message.getTags());
        this.messageStore.store(messageEntity);
    }

    private org.apache.rocketmq.common.message.Message createRocketMqMessage(
            String destination, Message<?> message) {
        Message<?> msg = this.doConvert(message.getPayload(), message.getHeaders(), null);
        return RocketMQUtil.convertToRocketMessage(getMessageConverter(), getCharset(),
                destination, msg);
    }
}
