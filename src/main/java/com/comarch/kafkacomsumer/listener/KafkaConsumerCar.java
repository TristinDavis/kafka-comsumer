package com.comarch.kafkacomsumer.listener;

import com.comarch.kafkacomsumer.model.Car;
import com.comarch.kafkacomsumer.model.Counter;
import com.comarch.kafkacomsumer.model.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerCar {

//    @KafkaListener(topics = "test", groupId = "group_json",
//            containerFactory = "byteKafkaListenerFactory")
//    public void consumeCar(String message) {
//        System.out.println(message);
//    }

    @KafkaListener(id = "id1", topics = "test2", groupId = "group_json",
            containerFactory = "kafkaListenerContainerFactory")
    public void consumeUser(ConsumerRecord message) throws IllegalArgumentException {
        System.out.println(Counter.COUNT + ": " + message.toString().split("key")[0]);
        Counter.COUNT++;
        throw new IllegalArgumentException();
    }

    @EventListener(condition = "event.listenerId.equals('id1')")
    public void eventHandler(ListenerContainerIdleEvent event) {
        System.out.println("Event:" + event.getListenerId());
    }

//    @KafkaListener(topics = "test2", groupId = "group_json2",
//            containerFactory = "userKafkaListenerFactory")
//    public void consume(User message) {
//        System.out.println(message);
//    }
}
