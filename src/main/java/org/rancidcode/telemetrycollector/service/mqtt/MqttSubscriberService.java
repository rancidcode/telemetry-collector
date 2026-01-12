package org.rancidcode.telemetrycollector.service.mqtt;

import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import jakarta.annotation.PostConstruct;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.rancidcode.telemetrycollector.config.MqttValidation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class MqttSubscriberService {

    final Mqtt5AsyncClient mqttClient;

    @Autowired
    MqttValidation mqttValidation;

    @Value("${mqtt.topic}")
    String topic;

    @Value("${mqtt.username}")
    String username;

    @Value("${mqtt.password}")
    String password;

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
                .callback(mqttValidation::messageValidation)
                .send()
                .whenComplete((subAck, subThrowable) -> {
                    if (subThrowable != null) {
                        log.info("MQTT subscriber failed : {}", subThrowable.getMessage());
                    } else {
                        log.info("MQTT subscriber success");
                    }
                });
    }
}