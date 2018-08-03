package com.comarch.kafkacomsumer.listener;

import com.comarch.kafkacomsumer.ErrorHandler.KafkaListenerErrorHandler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchErrorHandler;
import org.springframework.kafka.listener.ContainerAwareBatchErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


@Component
public class DlqBatchErrorHandler implements ContainerAwareBatchErrorHandler {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;



    public Object handleError(Message<?> message, ListenerExecutionFailedException exception, Consumer<?, ?> consumer) throws Exception {
        MessageHeaders headers = message.getHeaders();

        return null;
    }

    @Override
    public void handle(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer) {
        System.out.println("XD");
        data.forEach(System.out::println);
    }

    @Override
    public void handle(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer, MessageListenerContainer container) {
        System.out.println("XD");
        data.forEach(System.out::println);
//        List<String> topics = data.forEach()  .get(KafkaHeaders.RECEIVED_TOPIC, List.class);
//        List<Integer> partitions = headers.get(KafkaHeaders.RECEIVED_PARTITION_ID, List.class);
//        List<Long> offsets = headers.get(KafkaHeaders.OFFSET, List.class);
//        Map<TopicPartition, Long> offsetsToReset = new HashMap<>();
//        for (int i = 0; i < topics.size(); i++) {
//            int index = i;
//            offsetsToReset.compute(new TopicPartition(topics.get(i), partitions.get(i)),
//                    (k, v) -> v == null ? offsets.get(index) : Math.min(v, offsets.get(index)));
//        }
//        offsetsToReset.forEach((k, v) -> consumer.seek(k, v));
    }

//    @Override
//    public void handle(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer, MessageListenerContainer container) {
//        MessageHeaders headers = data. message.getHeaders();
//        List<String> topics = headers.get(KafkaHeaders.RECEIVED_TOPIC, List.class);
//        List<Integer> partitions = headers.get(KafkaHeaders.RECEIVED_PARTITION_ID, List.class);
//        List<Long> offsets = headers.get(KafkaHeaders.OFFSET, List.class);
//        Map<TopicPartition, Long> offsetsToReset = new HashMap<>();
//        for (int i = 0; i < topics.size(); i++) {
//            int index = i;
//            offsetsToReset.compute(new TopicPartition(topics.get(i), partitions.get(i)),
//                    (k, v) -> v == null ? offsets.get(index) : Math.min(v, offsets.get(index)));
//        }
//        offsetsToReset.forEach((k, v) -> consumer.seek(k, v));
//    }
}
