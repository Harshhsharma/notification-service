package com.example.notificationservice;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationConsumer {

    @KafkaListener(
            topics = "student-enrollment-events",
            groupId = "notification-group"
    )
    public void consume(String message) {

        System.out.println("Received message: " + message);
    }
}