import dto.FraudDto;
import dto.KeycloakDto;
import dto.UserDetailsDto;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import serdes.FraudJsonSerde;
import serdes.KeycloakJsonSerde;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.lang.System.getProperties;
import static org.junit.jupiter.api.Assertions.assertEquals;


class MyKafkaStreamTest {

    static Properties properties;
    KeycloakJsonSerde<KeycloakDto> jsonSerde = new KeycloakJsonSerde<>();
    FraudJsonSerde<FraudDto> fraudSerde = new FraudJsonSerde<>();
    Serde<String> stringSerde = Serdes.String();

    @BeforeAll
    static void setup() {
        properties = getProperties();
    }

    @Test
    void streamLocationFraude_Test() {
        Topology topology = Topologies.getTopologyLocationFraude("input", "output");
        TopologyTestDriver testDriver = new TopologyTestDriver(topology, properties);
        TestInputTopic<String, KeycloakDto> testInputTopic = testDriver.createInputTopic("input", stringSerde.serializer(), jsonSerde.serializer());
        TestOutputTopic<String, FraudDto> testOutputTopic = testDriver.createOutputTopic("output", stringSerde.deserializer(), fraudSerde.deserializer());

        KeycloakDto keycloakDto = new KeycloakDto();
        keycloakDto.setUsername("test");
        UserDetailsDto userDetailsDto = new UserDetailsDto();
        userDetailsDto.setUsername("test");
        keycloakDto.setType("LOGIN_ERROR");
        keycloakDto.setDetails(userDetailsDto);

        KeyValue keyValue = new KeyValue("test", keycloakDto);
        List<KeyValue<String, KeycloakDto>> keyValues = new ArrayList<>();
        keyValues.add(keyValue);
        keyValues.add(keyValue);
        keyValues.add(keyValue);
        keyValues.add(keyValue);
        keyValues.add(keyValue);
        keyValue = new KeyValue("test1", keycloakDto);
        keyValues.add(keyValue);


        testInputTopic.pipeKeyValueList(keyValues);

        assertEquals(2, testOutputTopic.getQueueSize());

    }
}