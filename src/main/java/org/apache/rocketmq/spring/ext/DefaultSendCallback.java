package org.apache.rocketmq.spring.ext;

import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.spring.ext.store.MessageEntity;
import org.apache.rocketmq.spring.ext.store.MessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DefaultSendCallback implements SendCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSendCallback.class);
    private static final int DEFAULT_DELAY_SEC = 1;

    private MessageStore messageStore;
    private Integer retryDelaySeconds;
    private ScheduledExecutorService scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("RocketMQSendCallback_"));

    @Override
    public void onSuccess(SendResult sendResult) {
        if (!SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
            LOGGER.info("SendStatus ({}) is not OK", sendResult.getSendStatus());
            return;
        }

        LOGGER.debug("Message is sent, update message status");
        this.updateMessageAsSent(sendResult.getMsgId());
    }

    private void updateMessageAsSent(String msgId) {
        if (messageStore == null) {
            return;
        }

        if (this.messageStore.update(msgId, MessageEntity.MessageStatus.SENT)) {
            // 如果更新失败延迟重试一次
            this.scheduledExecutorService.schedule(() -> {
                this.messageStore.update(msgId, MessageEntity.MessageStatus.SENT);
            }, retryDelaySeconds == null ? DEFAULT_DELAY_SEC : retryDelaySeconds, TimeUnit.SECONDS);
        }
    }

    @Override
    public void onException(Throwable e) {
        LOGGER.error("Send msg on Exception.", e);
    }
}
