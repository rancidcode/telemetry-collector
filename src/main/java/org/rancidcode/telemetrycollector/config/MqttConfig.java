package org.rancidcode.telemetrycollector.config;

import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

@Configuration
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class MqttConfig {

    @Value("${mqtt.host}")
    String mqttHost;

    @Value("${mqtt.port}")
    int mqttPort;

    @Bean
    public Mqtt5AsyncClient mqtt5Client() {
        return Mqtt5Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost(mqttHost)
                .serverPort(mqttPort)
                .sslWithDefaultConfig()
                .buildAsync();
    }
}