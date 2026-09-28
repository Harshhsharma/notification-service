package com.example.notificationservice.service;

import com.example.notificationservice.dto.CourseValidationEvent;
import com.example.notificationservice.dto.CourseValidationResponse;

public interface CourseValidationProducer {

    void sendCourseValidationRequest(CourseValidationEvent event);

    void sendCourseValidationResponse(CourseValidationResponse response);
}