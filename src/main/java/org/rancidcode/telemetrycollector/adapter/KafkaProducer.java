package org.rancidcode.telemetrycollector.adapter;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.rancidcode.telemetrycollector.domain.TelemetryValidator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class KafkaProducer {

    final KafkaTemplate<String, String> kafkaTemplate;
    TelemetryValidator telemetryValidator = new TelemetryValidator();

    @Value("${kafka.topic.raw}")
    String rawTopic;

    @Value("${kafka.topic.dlq}")
    String dlqTopic;

    public void processRawMessage(String rawMessage) {
        JSONObject message = telemetryValidator.getMessage(rawMessage);

        if (message == null) return;

        if (message.has("errorMessage")) {
            send(dlqTopic, message.toString());
            log.info("Topic: {}, Sent message: {}", dlqTopic, message);
        } else {
            send(rawTopic, message.toString());
            log.info("Topic: {}, Sent message: {}", rawTopic, message);
        }
    }

    public void send(String topic, String message) {
        kafkaTemplate.send(topic, message);
    }
}