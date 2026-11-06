package com.example.kafkalab.controller;

import com.example.kafkalab.producer.MessageProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/messages")
@RequiredArgsConstructor
public class MessageController {

    private final MessageProducer messageProducer;

    @PostMapping
    public ResponseEntity<Void> sendMessage(@RequestBody String message) {
        messageProducer.sendMessage(message);
        return ResponseEntity.accepted().build();
    }

}
