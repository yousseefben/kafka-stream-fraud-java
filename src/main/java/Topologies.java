import dto.DeviceDto;
import dto.FraudDto;
import dto.KeycloakDto;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import serdes.DeviceJsonSerde;
import serdes.FraudJsonSerde;
import serdes.KeycloakJsonSerde;
import utils.Utils;

import java.time.Duration;
import java.util.Arrays;

import static utils.Utils.getHashDevice;

interface Topologies {

    KeycloakJsonSerde<KeycloakDto> keycloakJsonSerde = new KeycloakJsonSerde<>();
    FraudJsonSerde<FraudDto> fraudSerde = new FraudJsonSerde<>();
    Serde<String> stringSerde = Serdes.String();
    DeviceJsonSerde<DeviceDto> deviceJsonSerde = new DeviceJsonSerde<>();

    String[] ELLIGIBLE_EVENT = {"LOGIN", "LOGIN_ERROR"};

    static Topology getTopologyLocationFraude(String inputTopic, String outputTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        builder
                .stream(inputTopic, Consumed.with(Serdes.String(), keycloakJsonSerde))
                .filter((key, value) -> value != null)
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
                .aggregate(FraudDto::new, Topologies::applyAggregator, Materialized.with(stringSerde, fraudSerde)).toStream()
                .map((Windowed<String> key, FraudDto fraud) -> new KeyValue<>(key.key(), fraud))
                .filter((k, v) -> v.getNbLoginFailure() > 3)
                .to(outputTopic, Produced.with(Serdes.String(), new FraudJsonSerde<>()));
        return builder.build();

    }

    static Topology getLoginAttemptsFromAuditKeycloakTopology(String inputTopic, String outputTopic) {
        StreamsBuilder builder = new StreamsBuilder();

        builder
                .stream(inputTopic, Consumed.with(stringSerde, keycloakJsonSerde))
                .filter((k, v) -> Arrays.asList(ELLIGIBLE_EVENT).contains(v.getType()))
                .mapValues(Utils::enhanceKeycloak)
                .to(outputTopic, Produced.with(stringSerde, keycloakJsonSerde));

        return builder.build();
    }

    static Topology getDeviceAttemptTopology(String inputTopic1, String inputTopic2, String outputTopic1, String outputTopic2) {
        StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, DeviceDto> knownDevices = builder.table(inputTopic1, Consumed.with(stringSerde, deviceJsonSerde));

        KStream<String, KeycloakDto> deviceStream = builder
                .stream(inputTopic2, Consumed.with(stringSerde, keycloakJsonSerde))
                .filter((key, value) -> value != null)
                .map((key, v) -> new KeyValue<>(v.getUsername() + ":" + getHashDevice(v.getDevice()), v))
                .leftJoin(knownDevices, (left, right) -> {
                    if (right == null) return left;
                    return null;
                })
                .filter((key, value) -> value != null);
        deviceStream.to(outputTopic1, Produced.with(stringSerde, keycloakJsonSerde));

        deviceStream
                .map((k, v) -> Utils.deviceFraudMapper(v))
                .to(outputTopic2, Produced.with(stringSerde, fraudSerde));

        return builder.build();
    }

    static FraudDto applyAggregator(String k, KeycloakDto v, FraudDto fraud) {
        fraud.setUsername(v.getDetails().getUsername());
        fraud.setIpAddress(v.getIpAddress());
        fraud.setDeviceDto(v.getDevice());
        if (v.getLat() != null && v.getLon() != null && fraud.getLat() != null && fraud.getLon() != null) {
            double dist = Utils.distance(v.getLat(), v.getLon(), fraud.getLat(), fraud.getLon());
            if (dist > fraud.getDistance()) fraud.setDistance(dist);
        }
        fraud.setLat(v.getLat());
        fraud.setLon(v.getLon());
        fraud.setTime(v.getTime());
        if ("LOGIN_ERROR".equals(v.type)) fraud.setNbLoginFailure(fraud.getNbLoginFailure() + 1);

        return fraud;
    }
}
