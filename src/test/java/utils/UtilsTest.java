package utils;

import dto.DeviceDto;
import dto.FraudDto;
import dto.KeycloakDto;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static utils.Utils.deviceFraudMapper;
import static utils.Utils.getHashDevice;

class UtilsTest {

    @Test
    void deviceFraudMapper_Test() {
        KeycloakDto keycloakDto = new KeycloakDto();
        DeviceDto deviceDto = new DeviceDto();
        deviceDto.setOs("myOs");
        keycloakDto.setIpAddress("192.168.1.1");
        keycloakDto.setUsername("youssef");
        keycloakDto.setTime(12345);
        keycloakDto.setDevice(deviceDto);

        KeyValue<String, FraudDto> result = deviceFraudMapper(keycloakDto);

        assertNotNull(result);
        assertNotNull(result.key);
        assertNotNull(result.value);
        assertEquals("youssef", result.key);
        FraudDto value = result.value;
        assertEquals(keycloakDto.getIpAddress(), value.getIpAddress());
        assertEquals(keycloakDto.getUsername(), value.getUsername());
        assertEquals(keycloakDto.getTime(), value.getTime());
        assertEquals(keycloakDto.getDevice().getOs(), value.getDeviceDto().getOs());

    }

    @Test
    void getHashDevice_Test() {
        DeviceDto deviceDto = new DeviceDto();
        deviceDto.setOs("myOs");
        deviceDto.setOsVersion("11.2");
        deviceDto.setMobile(false);
        deviceDto.setDevice("device");

        String encodedValue = Base64.getEncoder().encodeToString("myOs11.2devicefalse".getBytes(StandardCharsets.UTF_8));

        assertEquals(encodedValue, getHashDevice(deviceDto));
    }
}