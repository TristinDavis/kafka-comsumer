package com.comarch.kafkacomsumer.listener;

import com.comarch.kafkacomsumer.model.User;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    @KafkaListener(topics = "test", groupId = "group_json",
    containerFactory = "userKafkaListenerFactory")
    public void consume(User message) {
        System.out.println(message.toString());
    }
}
