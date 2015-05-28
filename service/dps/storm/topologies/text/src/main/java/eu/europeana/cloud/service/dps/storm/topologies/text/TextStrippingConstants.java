package eu.europeana.cloud.service.dps.storm.topologies.text;

/**
 * Constants for text stripping topology.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class TextStrippingConstants 
{
    public static final String KAFKA_INPUT_TOPIC = "text_stripping";
    public static final String INPUT_ZOOKEEPER = "192.168.47.129:2181";
    public static final String ZOOKEEPER_ROOT = "/" + KAFKA_INPUT_TOPIC; 
    
    public static final String KAFKA_OUTPUT_TOPIC = "extraction_topic";
    public static final String KAFKA_OUTPUT_BROKER = "192.168.47.129:9093";
    
    public static final String KAFKA_METRICS_TOPIC = "storm_metrics_topic";
    public static final String KAFKA_METRICS_BROKER = "192.168.47.129:9093";
    
    public static final String PROGRESS_ZOOKEEPER = "192.168.47.129:2181";

    public static final String MCS_URL = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT";
    public static final String USERNAME = "admin";
    public static final String PASSWORD = "admin";
 
    // ------ PARALLELISM HINTS ------
    public static final Integer KAFKA_SPOUT_PARALLEL = 1;
    public static final Integer PARSE_TASKS_BOLT_PARALLEL = 1;
    public static final Integer DATASET_BOLT_PARALLEL = 1;
    public static final Integer FILE_BOLT_PARALLEL = 1;
    public static final Integer EXTRACT_BOLT_PARALLEL = 1;
    public static final Integer STORE_BOLT_PARALLEL = 1;
    public static final Integer INFORM_BOLT_PARALLEL = 1;
    public static final Integer PROGRESS_BOLT_PARALLEL = 1;
    
    public static final Integer METRICS_CONSUMER_PARALLEL = 1;
}