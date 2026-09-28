package com.example.notificationservice;

import com.example.notificationservice.dto.CourseValidationEvent;
import com.example.notificationservice.dto.CourseValidationResponse;
import com.example.notificationservice.service.CourseValidationProducer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationConsumer {

    private final CourseValidationProducer producer;

    public NotificationConsumer(CourseValidationProducer producer) {
        this.producer = producer;
    }

    @KafkaListener(
            topics = "student-enrollment-events",
            groupId = "notification-group"
    )
    public void consume(CourseValidationEvent event) {

        System.out.println(
                "Received enrollment event: student="
                        + event.getStudentId()
                        + ", course="
                        + event.getCourseId()
        );
    }
    @KafkaListener(
            topics = "course-validation-request",
            groupId = "notification-validation-group"
    )
    public void consumeCourseValidationRequest(
            CourseValidationEvent event) {

        System.out.println(
                "Course validation request received: "
                        + event.getCourseId()
        );

        producer.sendCourseValidationRequest(event);
    }

    @KafkaListener(
            topics = "course-validation-response",
            groupId = "notification-response-group",
            containerFactory = "responseKafkaListenerContainerFactory"
    )
    public void consumeCourseValidationResponse(
            CourseValidationResponse response) {

        System.out.println(
                "Course validation response received: "
                        + response.isCourseExists()
        );

        producer.sendCourseValidationResponse(response);
    }
}
