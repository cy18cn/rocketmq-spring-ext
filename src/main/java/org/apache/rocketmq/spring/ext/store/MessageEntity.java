package org.apache.rocketmq.spring.ext.store;

import lombok.Data;
import lombok.Getter;

import java.util.Date;

@Data
public class MessageEntity {
    private Long id;
    private String msgId;
    private Integer status;
    private String content;
    private String topic;
    private String tags;
    private String headers;
    private Date createdAt;

    public enum MessageStatus {
        SENDING(0),
        SENT(1),
        CONSUMED(2);

        @Getter
        private Integer val;

        MessageStatus(Integer val) {
            this.val = val;
        }
    }
}
