package serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import dto.KeycloakDto;
import org.apache.commons.lang.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class KeycloakJsonSerde<T extends KeycloakDto> implements Serializer<T>, Deserializer<T>, Serde<T> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final Logger logger = LoggerFactory.getLogger(KeycloakJsonSerde.class);

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(final String topic, final byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            return (T) OBJECT_MAPPER.readValue(data, KeycloakDto.class);
        } catch (final IOException e) {
            logger.error("Error deserializing JSON message", e);

            throw new SerializationException(e);
        }
    }

    @Override
    public byte[] serialize(final String topic, final T data) {
        if (data == null) {
            return null;
        }

        try {
            return OBJECT_MAPPER.writeValueAsBytes(data);
        } catch (final Exception e) {
            logger.error("Error serializing JSON message", e);
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }
}


