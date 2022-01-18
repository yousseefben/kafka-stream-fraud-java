import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static utils.Utils.getPropsValues;


public class TestSTreamApp {

    private static final Logger logger = LoggerFactory.getLogger(TestSTreamApp.class);


    public static void main(String[] args) {

        MyKafkaStream myKafkaStream = new MyKafkaStream(getPropsValues());

        logger.info("Kafka Streams Started");
        myKafkaStream.getLoginAttemptsFromAuditKeycloak().start();
        myKafkaStream.streamLocationFraude().start();
        myKafkaStream.deviceAttemptStream().start();

    }


}


