package org.rancidcode.telemetrycollector.service.mqtt;

import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import jakarta.annotation.PostConstruct;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.json.JSONException;
import org.json.JSONObject;
import org.rancidcode.telemetrycollector.service.kafka.KafkaProducerService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.Instant;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class MqttSubscriberService {

    final Mqtt5AsyncClient mqttClient;
    final KafkaProducerService kafkaProducer;

    @Value("${mqtt.topic}")
    String topic;

    @Value("${mqtt.username}")
    String username;

    @Value("${mqtt.password}")
    String password;

    @Value("${kafka.topic.raw}")
    String rawTopic;

    @Value("${kafka.topic.dlq}")
    String dlqTopic;

    @PostConstruct
    public void init() {
        mqttClient.connectWith()
                .simpleAuth()
                .username(username)
                .password(password.getBytes(StandardCharsets.UTF_8))
                .applySimpleAuth().send()
                .whenComplete((connAck, throwable) -> {
                    if (throwable != null) {
                        log.info("MQTT connection failed : {}", throwable.getMessage());
                    } else {
                        log.info("MQTT subscriber connected");
                        subscribeToTopic();
                    }
                });
    }

    private void subscribeToTopic() {
        mqttClient.subscribeWith()
                .topicFilter(topic)
                .callback(this::messageValidation)
                .send()
                .whenComplete((subAck, subThrowable) -> {
                    if (subThrowable != null) {
                        log.info("MQTT subscriber failed : {}", subThrowable.getMessage());
                    } else {
                        log.info("MQTT subscriber success");
                    }
                });
    }

    private void messageValidation(Mqtt5Publish publish) {
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