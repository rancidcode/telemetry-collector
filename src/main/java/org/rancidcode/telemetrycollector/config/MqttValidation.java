package org.rancidcode.telemetrycollector.config;

import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.json.JSONObject;
import org.rancidcode.telemetrycollector.service.kafka.KafkaProducerService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;

@Component
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class MqttValidation {

    final KafkaProducerService kafkaProducer;

    @Value("${kafka.topic.raw}")
    String rawTopic;

    @Value("${kafka.topic.dlq}")
    String dlqTopic;

    public void messageValidation(Mqtt5Publish publish) {
        String message = new String(publish.getPayloadAsBytes(), StandardCharsets.UTF_8);

        try {
            JSONObject jsonObject = new JSONObject(message);

            kafkaProducer.send(rawTopic, jsonObject.toString());
            log.info("Topic: {}, Sent message: {}", rawTopic, jsonObject);
        } catch (JSONException e) {
            JSONObject dlq = new JSONObject();
            getDlq(dlq, e, message);

            kafkaProducer.send(dlqTopic, dlq.toString());
            log.info("Topic: {}, Sent message: {}", dlqTopic, dlq);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void getDlq(JSONObject dlq, JSONException e, String message) {
        Instant nowUtc = Instant.now();
        nowUtc.atOffset(ZoneOffset.of("+07:00"));

        dlq.put("timestamp", nowUtc);
        dlq.put("dlgType", "INVALID_JSON");
        dlq.put("source", "MQTT");
        dlq.put("rawMessage", message);
        dlq.put("errorMessage", e.getMessage());
    }
}
