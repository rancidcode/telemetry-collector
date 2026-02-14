package org.rancidcode.telemetrycollector.adapter;

import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import jakarta.annotation.PostConstruct;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.rancidcode.telemetrycollector.infra.MqttStatusPublisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class MqttSubscriber {

    final Mqtt5AsyncClient mqttClient;
    final KafkaProducer kafkaProducer;
    final MqttStatusPublisher mqttStatusPublisher;

    @Value("${mqtt.topic}")
    String topic;

    @Value("${mqtt.username}")
    String username;

    @Value("${mqtt.password}")
    String password;

    @Value("${webhook.url}")
    String webhookUrl;

    @PostConstruct
    public void init() {
        mqttClient.connectWith()
                .simpleAuth()
                .username(username)
                .password(password.getBytes(StandardCharsets.UTF_8))
                .applySimpleAuth().send()
                .whenComplete((connAck, throwable) -> {
                    if (throwable != null) {
                        log.error("MQTT connection failed : {}", throwable.getMessage());
                        mqttStatusPublisher.publishMqttStatus(webhookUrl, "DISCONNECTED");
                    } else {
                        log.info("MQTT subscriber connected");
                        mqttStatusPublisher.publishMqttStatus(webhookUrl, "CONNECTED");
                        subscribeToTopic();
                    }
                });
    }

    private void subscribeToTopic() {
        mqttClient.subscribeWith()
                .topicFilter(topic)
                .callback(this::produce)
                .send()
                .whenComplete((subAck, subThrowable) -> {
                    if (subThrowable != null) {
                        log.error("MQTT subscriber failed : {}", subThrowable.getMessage());
                    } else {
                        log.info("MQTT subscriber success");
                    }
                });
    }

    private void produce(Mqtt5Publish publish) {
        String message = new String(publish.getPayloadAsBytes(), StandardCharsets.UTF_8);
        kafkaProducer.processRawMessage(message);
    }
}