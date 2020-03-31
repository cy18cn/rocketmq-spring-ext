package org.apache.rocketmq.spring.ext.store;

import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class MemoryMessageStore implements MessageStore {
    private Map<String, MessageEntity> store;
    private ReentrantLock lock = new ReentrantLock();

    public MemoryMessageStore(Map<String, MessageEntity> store) {
        this.store = store;
    }

    @Override
    public void store(MessageEntity message) {
        lock.lock();
        try {
            store.put(message.getMsgId(), message);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean update(String msgId, MessageEntity.MessageStatus messageStatus) {
        lock.lock();
        try {
            final MessageEntity msg = store.get(msgId);
            msg.setStatus(messageStatus.getVal());
            store.put(msgId, msg);
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void remove(String msgId) {
        lock.lock();
        try {
            store.remove(msgId);
        } finally {
            lock.unlock();
        }
    }
}
