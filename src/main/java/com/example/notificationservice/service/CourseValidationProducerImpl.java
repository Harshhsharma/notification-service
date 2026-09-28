package com.example.notificationservice.service;

import com.example.notificationservice.dto.CourseValidationEvent;
import com.example.notificationservice.dto.CourseValidationResponse;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class CourseValidationProducerImpl
        implements CourseValidationProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public CourseValidationProducerImpl(
            KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void sendCourseValidationRequest(
            CourseValidationEvent event) {

        System.out.println(
                "🔥 SENDING TO COURSE: " + event.getCourseId()
        );

        kafkaTemplate.send(
                "course-validation-to-course",
                event
        ).whenComplete((result, ex) -> {

            if (ex != null) {
                System.out.println(
                        "❌ FAILED TO SEND TO COURSE: "
                                + ex.getMessage()
                );
            } else {
                System.out.println(
                        "✅ SENT TO COURSE | topic="
                                + result.getRecordMetadata().topic()
                                + " partition="
                                + result.getRecordMetadata().partition()
                                + " offset="
                                + result.getRecordMetadata().offset()
                );
            }
        });
    }

    @Override
    public void sendCourseValidationResponse(
            CourseValidationResponse response) {

        kafkaTemplate.send(
                "course-validation-to-student",
                response
        );
    }
}