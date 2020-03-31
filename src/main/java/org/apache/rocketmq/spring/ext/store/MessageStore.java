package org.apache.rocketmq.spring.ext.store;

public interface MessageStore {
    void store(MessageEntity message);

    boolean update(String msgId, MessageEntity.MessageStatus messageStatus);

    void remove(String msgId);
}
