package com.schooldevops.kafka.stream.aggregatingwordcntdemo.producer;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Slf4j
@AllArgsConstructor
@Component
public class KafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String message) {
        kafkaTemplate.send("input-topic", message)
                .addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
                    @Override
                    public void onSuccess(final SendResult<String, String> message) {
                        log.info("sent message= " + message + " with offset= " + message.getRecordMetadata().offset());
                    }

                    @Override
                    public void onFailure(final Throwable throwable) {
                        log.error("unable to send message= " + message, throwable);
                    }
            });
    }
}
