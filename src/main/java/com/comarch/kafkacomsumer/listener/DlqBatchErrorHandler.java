package com.comarch.kafkacomsumer.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerAwareBatchErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;


@Component
public class DlqBatchErrorHandler implements ContainerAwareBatchErrorHandler {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    @Override
    public void handle(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer, MessageListenerContainer container) {
        for (ConsumerRecord consumerRecord : data) {
            System.out.println("XD " + consumerRecord);
        }
//        Map<TopicPartition, Long> offsets = new LinkedHashMap<>();
//        data.forEach(r -> offsets.computeIfAbsent(new TopicPartition(r.topic(), r.partition()), k -> r.offset()));
//        offsets.forEach(consumer::seek);
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
