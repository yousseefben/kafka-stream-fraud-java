package utils;

import dto.DeviceDto;
import dto.FraudDto;
import dto.KeycloakDto;
import org.apache.kafka.streams.KeyValue;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

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


}
