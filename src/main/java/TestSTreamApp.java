import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class TestSTreamApp {

    private static final Logger logger = LoggerFactory.getLogger(TestSTreamApp.class);


    public static void main(String[] args) {

        MyKafkaStream myKafkaStream = new MyKafkaStream(getPropsValues());

        logger.info("Kafka Streams Started");
        myKafkaStream.getLoginAttemptsFromAuditKeycloak().start();
        myKafkaStream.streamLocationFraude().start();
        myKafkaStream.deviceAttemptStream().start();

    }

    static Properties getPropsValues() {
        Properties prop = new Properties();
        try (InputStream input = TestSTreamApp.class.getClassLoader().getResourceAsStream("config.properties")) {
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


