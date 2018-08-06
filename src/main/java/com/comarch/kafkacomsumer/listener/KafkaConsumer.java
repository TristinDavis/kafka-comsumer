package com.comarch.kafkacomsumer.listener;

import com.comarch.kafkacomsumer.ErrorHandler.KafkaListenerErrorHandler;
import com.comarch.kafkacomsumer.model.Car;
import com.comarch.kafkacomsumer.model.Counter;
import com.comarch.kafkacomsumer.model.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;

@Service
public class KafkaConsumer {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static int countBatch = 0;

//    @KafkaListener(topics = "test", groupId = "group_json",
//            containerFactory = "byteKafkaListenerFactory")
//    public void consumeCar(String message) {
//        System.out.println(message);
//    }

    @KafkaListener(id = "id2", topics = "test", groupId = "group_json",
            containerFactory = "kafkaListenerContainerFactory")
    public void consumeUser(ConsumerRecord message) {
        System.out.println(Counter.COUNT + ": " + message.toString().split("key")[0]);
        Counter.COUNT++;
    }

    @KafkaListener(id = "id3", topics = "test3", groupId = "group_json",
            containerFactory = "batchKafkaListenerContainerFactory", errorHandler = "listen10ErrorHandler")
    public void consumeUsers(List<User> messages,
                             @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) List<Integer> keys,
                             @Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions,
                             @Header(KafkaHeaders.RECEIVED_TOPIC) List<String> topics,
                             @Header(KafkaHeaders.OFFSET) List<Long> offsets) throws GeneralSecurityException {
        for (User message: messages) {
            countBatch++;
            if (countBatch % 3 == 0) {
                System.out.println("Throwing exception for countBatch " + countBatch + " msg: " + offsets.get(countBatch-1) + " " + (message == null ? "" : message));
                throw new GeneralSecurityException();
            }
            else {
                System.out.println(offsets.get(countBatch-1) + " " + message);
            }
        }
        System.out.println("End of Batch");
        System.exit(0);
        countBatch = 1;
    }

    @EventListener(condition = "event.listenerId.equals('id2')")
    public void eventHandler(ListenerContainerIdleEvent event) {
        System.out.println("Event:" + event.getListenerId());
    }

//    @KafkaListener(topics = "test2", groupId = "group_json2",
//            containerFactory = "userKafkaListenerFactory")
//    public void consume(User message) {
//        System.out.println(message);
//    }
}
