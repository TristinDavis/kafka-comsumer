package com.comarch.kafkacomsumer.listener;

import com.comarch.kafkacomsumer.model.Car;
import com.comarch.kafkacomsumer.model.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerCar {
    private static final ObjectMapper mapper = new ObjectMapper();

//    @KafkaListener(topics = "test", groupId = "group_json",
//            containerFactory = "byteKafkaListenerFactory")
//    public void consumeCar(String message) {
//        System.out.println(message);
//    }

    @KafkaListener(id = "id_1", topics = "test2", groupId = "group_json",
            containerFactory = "kafkaListenerContainerFactory")
    public void consumeUser(Car message) {
        System.out.println(message);
    }

//    @KafkaListener(topics = "test2", groupId = "group_json2",
//            containerFactory = "userKafkaListenerFactory")
//    public void consume(User message) {
//        System.out.println(message);
//    }
}
