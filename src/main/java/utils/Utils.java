package utils;

import dto.DeviceDto;
import dto.FraudDto;
import dto.KeycloakDto;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import serdes.KeycloakJsonSerde;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;

public interface Utils {

    static KeyValue<String, FraudDto> deviceFraudMapper(KeycloakDto keycloakDto) {
        FraudDto fraudDto = new FraudDto();
        fraudDto.setDeviceDto(keycloakDto.getDevice());
        fraudDto.setUsername(keycloakDto.getUsername());
        fraudDto.setIpAddress(keycloakDto.getIpAddress());
        fraudDto.setTime(keycloakDto.getTime());
        return new KeyValue<>(keycloakDto.getUsername(), fraudDto);
    }

    static String getHashDevice(DeviceDto deviceDto) {
        String deviceHash = deviceDto.getOs() + deviceDto.getOsVersion() + deviceDto.getDevice() + deviceDto.isMobile();
        return Base64.getEncoder().encodeToString(deviceHash.getBytes(StandardCharsets.UTF_8));
    }

    static Properties getProperties(String bootstrapServer) {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KeycloakJsonSerde.class);

        return streamsConfiguration;
    }
}
