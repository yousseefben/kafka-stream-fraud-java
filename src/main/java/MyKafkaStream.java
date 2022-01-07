import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

import static utils.Utils.getProperties;

public class MyKafkaStream {
    String LOGIN_ATTEMPT_TOPIC;
    String MALICIOUS_ATTEMPT_TOPIC;
    String NEW_DEVICE_ATTEMPT_TOPIC;
    String KNOWN_DEVICE_TOPIC;
    String KEYCLOAK_EVENT_TOPIC;
    String BOOTSTRAP_SERVERS;

    Properties props;

    public MyKafkaStream(Properties propertiesSystem) {
        LOGIN_ATTEMPT_TOPIC = propertiesSystem.getProperty("LOGIN_ATTEMPT_TOPIC");
        MALICIOUS_ATTEMPT_TOPIC = propertiesSystem.getProperty("MALICIOUS_ATTEMPT_TOPIC");
        NEW_DEVICE_ATTEMPT_TOPIC = propertiesSystem.getProperty("NEW_DEVICE_ATTEMPT_TOPIC");
        KNOWN_DEVICE_TOPIC = propertiesSystem.getProperty("KNOWN_DEVICE_TOPIC");
        KEYCLOAK_EVENT_TOPIC = propertiesSystem.getProperty("KEYCLOAK_EVENT_TOPIC");
        BOOTSTRAP_SERVERS = propertiesSystem.getProperty("BOOTSTRAP_SERVERS");
        props = getProperties(BOOTSTRAP_SERVERS);
    }


    public KafkaStreams getLoginAttemptsFromAuditKeycloak() {
        Properties properties = props;
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream");

        Topology topology = Topologies.getLoginAttemptsFromAuditKeycloakTopology(KEYCLOAK_EVENT_TOPIC, LOGIN_ATTEMPT_TOPIC);

        KafkaStreams kafkaStreams = new KafkaStreams(
                topology,
                properties);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        return kafkaStreams;
    }


    public KafkaStreams streamLocationFraude() {
        Properties properties = props;
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-loc");

        Topology topology = Topologies.getTopologyLocationFraude(LOGIN_ATTEMPT_TOPIC, MALICIOUS_ATTEMPT_TOPIC);

        KafkaStreams kafkaStreams = new KafkaStreams(
                topology,
                properties);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        return kafkaStreams;
    }

    KafkaStreams deviceAttemptStream() {
        Properties properties = props;
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-attempt-new-device");

        Topology topology = Topologies.getDeviceAttemptTopology(KNOWN_DEVICE_TOPIC, LOGIN_ATTEMPT_TOPIC, NEW_DEVICE_ATTEMPT_TOPIC, MALICIOUS_ATTEMPT_TOPIC);

        KafkaStreams kafkaStreams = new KafkaStreams(
                topology,
                properties);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        return kafkaStreams;
    }


}
