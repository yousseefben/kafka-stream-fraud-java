import dto.DeviceDto;
import dto.FraudDto;
import dto.KeycloakDto;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import serdes.DeviceJsonSerde;
import serdes.FraudJsonSerde;
import serdes.KeycloakJsonSerde;
import utils.Utils;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static utils.Utils.getHashDevice;

public class MyKafkaStream {
    String LOGIN_ATTEMPT_TOPIC;
    String MALICIOUS_ATTEMPT_TOPIC;
    String NEW_DEVICE_ATTEMPT_TOPIC;
    String KNOWN_DEVICE_TOPIC;
    String BOOTSTRAP_SERVERS;

    public MyKafkaStream(Properties propertiesSystem) {
        LOGIN_ATTEMPT_TOPIC = propertiesSystem.getProperty("LOGIN_ATTEMPT_TOPIC");
        MALICIOUS_ATTEMPT_TOPIC = propertiesSystem.getProperty("MALICIOUS_ATTEMPT_TOPIC");
        NEW_DEVICE_ATTEMPT_TOPIC = propertiesSystem.getProperty("NEW_DEVICE_ATTEMPT_TOPIC");
        KNOWN_DEVICE_TOPIC = propertiesSystem.getProperty("KNOWN_DEVICE_TOPIC");
        BOOTSTRAP_SERVERS = propertiesSystem.getProperty("BOOTSTRAP_SERVERS");
    }

    final Logger logger = LoggerFactory.getLogger(MyKafkaStream.class);


    String[] ELLIGIBLE_EVENT = {"LOGIN", "LOGIN_ERROR"};

    public KafkaStreams getLoginAttemptsFromAuditKeycloak() {
        Properties properties = getProperties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream");
        Serde<String> stringSerde = Serdes.String();
        KeycloakJsonSerde<KeycloakDto> jsonSerde = new KeycloakJsonSerde<>();
        StreamsBuilder builder = new StreamsBuilder();
        builder
                .stream("keycloak-event", Consumed.with(stringSerde, jsonSerde))
                .peek((k, v) -> logger.info("Observed key:{} event: {}", k, v.toString()))
                .filter((k, v) -> Arrays.asList(ELLIGIBLE_EVENT).contains(v.getType()))
                .peek((k, v) -> logger.info("Transformed event: {}", v))
                .to(LOGIN_ATTEMPT_TOPIC, Produced.with(stringSerde, jsonSerde));

        KafkaStreams kafkaStreams = new KafkaStreams(
                builder.build(),
                properties);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        return kafkaStreams;
    }


    public KafkaStreams streamLocationFraude() {
        Properties properties = getProperties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-loc");

        Serde<String> stringSerde = Serdes.String();
        KeycloakJsonSerde<KeycloakDto> jsonSerde = new KeycloakJsonSerde<>();
        FraudJsonSerde<FraudDto> fraudSerde = new FraudJsonSerde<>();
        StreamsBuilder builder = new StreamsBuilder();
        builder
                .stream(LOGIN_ATTEMPT_TOPIC, Consumed.with(stringSerde, jsonSerde))
                .filter((key, value) -> value != null)
                .peek((k, v) -> logger.info("event fraude key : {} value : {}", k, v))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(FraudDto::new, (k, v, fraud) -> {
                    fraud.setUsername(v.getDetails().getUsername());
                    fraud.setIpAddress(v.getIpAddress());
                    fraud.setDeviceDto(v.getDevice());
                    if ("LOGIN_ERROR".equals(v.type)) fraud.setNbLoginFailure(fraud.getNbLoginFailure() + 1);
                    return fraud;
                }, Materialized.with(stringSerde, fraudSerde)).toStream()
                .map((Windowed<String> key, FraudDto fraud) -> new KeyValue<>(key.key(), fraud))
                .filter((k, v) -> v.getNbLoginFailure() > 3)
                .to(MALICIOUS_ATTEMPT_TOPIC, Produced.with(stringSerde, fraudSerde));

        KafkaStreams kafkaStreams = new KafkaStreams(
                builder.build(),
                properties);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        return kafkaStreams;
    }

    KafkaStreams deviceAttemptStream() {
        Properties properties = getProperties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-attempt-new-device");

        Serde<String> stringSerde = Serdes.String();
        DeviceJsonSerde<DeviceDto> deviceJsonSerde = new DeviceJsonSerde<>();
        KeycloakJsonSerde<KeycloakDto> keycloakJsonSerde = new KeycloakJsonSerde<>();
        FraudJsonSerde<FraudDto> fraudSerde = new FraudJsonSerde<>();
        StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, DeviceDto> knownDevices = builder.table(KNOWN_DEVICE_TOPIC, Consumed.with(stringSerde, deviceJsonSerde));

        KStream<String, KeycloakDto> deviceStrem = builder
                .stream(LOGIN_ATTEMPT_TOPIC, Consumed.with(stringSerde, keycloakJsonSerde))
                .filter((key, value) -> value != null)
                .map((key, v) -> new KeyValue<>(v.getUsername() + ":" + getHashDevice(v.getDevice()), v))
                .leftJoin(knownDevices, (left, right) -> {
                    if (right == null) return left;
                    return null;
                })
                .filter((key, value) -> value != null)
                .peek((k, v) -> logger.info("new device attempt key: {} value : {}", k, v));
        deviceStrem.to(NEW_DEVICE_ATTEMPT_TOPIC, Produced.with(stringSerde, keycloakJsonSerde));
        deviceStrem
                .map((k, v) -> Utils.deviceFraudMapper(v))
                .to(MALICIOUS_ATTEMPT_TOPIC, Produced.with(stringSerde, fraudSerde));

        KafkaStreams kafkaStreams = new KafkaStreams(
                builder.build(),
                properties);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        return kafkaStreams;
    }


    private Properties getProperties() {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return streamsConfiguration;
    }

}
