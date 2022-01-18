package utils;

import com.maxmind.geoip2.WebServiceClient;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import dto.DeviceDto;
import dto.FraudDto;
import dto.KeycloakDto;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import serdes.KeycloakJsonSerde;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;

public interface Utils {
    Logger logger = LoggerFactory.getLogger(Utils.class);
    Properties props = getPropsValues();


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

    static double distance(double lat1, double lon1, double lat2, double lon2) {
        if ((lat1 == lat2) && (lon1 == lon2)) {
            return 0;
        } else {
            double theta = lon1 - lon2;
            double dist = Math.sin(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2)) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(theta));
            dist = Math.acos(dist);
            dist = Math.toDegrees(dist);
            dist = dist * 60 * 1.1515;
            //to KM
            dist = dist * 1.609344;

            return (dist);
        }
    }

    static CityResponse getGeoloc(String ip) {
        String MAXMIND_LICENCE_KEY = props.getProperty("MAXMIND_LICENCE_KEY");
        String MAXMIND_ACCOUNT_ID = props.getProperty("MAXMIND_ACCOUNT_ID");

        WebServiceClient client = new WebServiceClient.Builder(Integer.parseInt(MAXMIND_ACCOUNT_ID), MAXMIND_LICENCE_KEY).host("geolite.info").build();
        try {
            InetAddress ipAddress = InetAddress.getByName(ip);
            return client.city(ipAddress);
        } catch (IOException | GeoIp2Exception e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    static KeycloakDto enhanceKeycloak(KeycloakDto keycloakDto) {
        CityResponse city = getGeoloc(keycloakDto.getIpAddress());
        if (city != null) {
            keycloakDto.setCity(city.getCity().getName());
            keycloakDto.setLat(city.getLocation().getLatitude());
            keycloakDto.setLat(city.getLocation().getLongitude());
        }
        return keycloakDto;
    }

    static Properties getPropsValues() {
        Properties prop = new Properties();
        try (InputStream input = Utils.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input != null) {
                prop.load(input);
                return prop;
            }
            logger.error("unable to find config.properties");
        } catch (IOException ex) {
            logger.error("error when reading properties: {}", ex.getMessage());
        }
        return prop;
    }
}
