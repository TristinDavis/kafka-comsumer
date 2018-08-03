package com.comarch.kafkacomsumer.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerAwareErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.List;


@Component
public class DlqErrorHandler implements ContainerAwareErrorHandler {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Override
    public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer, MessageListenerContainer container) {
        ConsumerRecord<?, ?> record = records.get(0);
        try {
            System.out.println("In try block");
            kafkaTemplate.send("dlqTopic", record.value().toString());
            consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
        } catch (Exception e) {
            System.out.println("In catch block");
            consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
            throw new KafkaException("Seek to current after exception", thrownException);
        }
    }
}
