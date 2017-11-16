package eu.europeana.cloud.service.dps.storm.topologies.oaipmh;

import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.storm.ParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.io.AddResultToDataSetBolt;
import eu.europeana.cloud.service.dps.storm.io.OAIWriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.io.RevisionWriterBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.IdentifiersHarvestingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.RecordHarvestingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.TaskSplittingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.*;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.RECORD_HARVESTING_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.SPOUT;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

/**
 *
 */
public class OAIPHMHarvestingTopology {
    private static Properties topologyProperties;
    private final BrokerHosts brokerHosts;
    private static final String TOPOLOGY_PROPERTIES_FILE = "oai-topology-config.properties";
    private final String REPOSITORY_STREAM = InputDataType.REPOSITORY_URLS.name();

    public OAIPHMHarvestingTopology(String defaultPropertyFile, String providedPropertyFile) {
        topologyProperties = new Properties();
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
        brokerHosts = new ZkHosts(topologyProperties.getProperty(INPUT_ZOOKEEPER_ADDRESS));
    }

    public final StormTopology buildTopology(String oaiTopic, String ecloudMcsAddress, String uisAddress) {
        Map<String, String> routingRules = new HashMap<>();
        routingRules.put(REPOSITORY_STREAM, REPOSITORY_STREAM);


        WriteRecordBolt writeRecordBolt = new OAIWriteRecordBolt(ecloudMcsAddress, uisAddress);
        RevisionWriterBolt revisionWriterBolt = new RevisionWriterBolt(ecloudMcsAddress);
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, oaiTopic, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        // kafkaConfig.ignoreZkOffsets = true;
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);


        builder.setSpout(SPOUT, kafkaSpout, (getAnInt(KAFKA_SPOUT_PARALLEL)))
                .setNumTasks((getAnInt(KAFKA_SPOUT_NUMBER_OF_TASKS)));

        builder.setBolt(PARSE_TASK_BOLT, new ParseTaskBolt(routingRules),
                getAnInt(PARSE_TASKS_BOLT_PARALLEL))
                .setNumTasks(getAnInt(PARSE_TASKS_BOLT_NUMBER_OF_TASKS))
                .shuffleGrouping(SPOUT);

        builder.setBolt(TASK_SPLITTING_BOLT, new TaskSplittingBolt(parseLong(topologyProperties.getProperty(INTERVAL))),
                (getAnInt(TASK_SPLITTING_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(TASK_SPLITTING_BOLT_BOLT_NUMBER_OF_TASKS)))
                .shuffleGrouping(PARSE_TASK_BOLT, REPOSITORY_STREAM);


        builder.setBolt(IDENTIFIERS_HARVESTING_BOLT, new IdentifiersHarvestingBolt(),
                (getAnInt(IDENTIFIERS_HARVESTING_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(IDENTIFIERS_HARVESTING_BOLT_NUMBER_OF_TASKS)))
                .shuffleGrouping(TASK_SPLITTING_BOLT);


        builder.setBolt(RECORD_HARVESTING_BOLT, new RecordHarvestingBolt(),
                (getAnInt(RECORD_HARVESTING_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(RECORD_HARVESTING_BOLT_NUMBER_OF_TASKS)))
                .shuffleGrouping(IDENTIFIERS_HARVESTING_BOLT);

        builder.setBolt(WRITE_RECORD_BOLT, writeRecordBolt,
                (getAnInt(WRITE_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(WRITE_BOLT_NUMBER_OF_TASKS)))
                .shuffleGrouping(RECORD_HARVESTING_BOLT);

        builder.setBolt(REVISION_WRITER_BOLT, revisionWriterBolt,
                (getAnInt(REVISION_WRITER_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(Revision_WRITER_BOLT_NUMBER_OF_TASKS)))
                .shuffleGrouping(WRITE_RECORD_BOLT);


        AddResultToDataSetBolt addResultToDataSetBolt = new AddResultToDataSetBolt(ecloudMcsAddress);
        builder.setBolt(WRITE_TO_DATA_SET_BOLT, addResultToDataSetBolt,
                (getAnInt(ADD_TO_DATASET_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(ADD_TO_DATASET_BOLT_NUMBER_OF_TASKS)))
                .shuffleGrouping(REVISION_WRITER_BOLT);
        return builder.createTopology();
    }

    private static int getAnInt(String parseTasksBoltParallel) {
        return parseInt(topologyProperties.getProperty(parseTasksBoltParallel));
    }

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

        if (args.length <= 1) {

            String providedPropertyFile = "";
            if (args.length == 1) {
                providedPropertyFile = args[0];
            }
            OAIPHMHarvestingTopology oaiphmHarvestingTopology = new OAIPHMHarvestingTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);
            String topologyName = topologyProperties.getProperty(TOPOLOGY_NAME);

            String kafkaTopic = topologyName;
            String ecloudMcsAddress = topologyProperties.getProperty(MCS_URL);
            String ecloudUisAddress = topologyProperties.getProperty(UIS_URL);
            StormTopology stormTopology = oaiphmHarvestingTopology.buildTopology(kafkaTopic, ecloudMcsAddress, ecloudUisAddress);
            config.setNumWorkers(getAnInt(WORKER_COUNT));
            config.setMaxTaskParallelism(
                    getAnInt(MAX_TASK_PARALLELISM));
            config.put(Config.NIMBUS_THRIFT_PORT,
                    getAnInt(THRIFT_PORT));
            config.put(topologyProperties.getProperty(INPUT_ZOOKEEPER_ADDRESS),
                    topologyProperties.getProperty(INPUT_ZOOKEEPER_PORT));
            config.put(Config.NIMBUS_SEEDS, Arrays.asList(topologyProperties.getProperty(NIMBUS_SEEDS)));
            config.put(Config.STORM_ZOOKEEPER_SERVERS,
                    Arrays.asList(topologyProperties.getProperty(STORM_ZOOKEEPER_ADDRESS)));

            StormSubmitter.submitTopology(topologyName, config, stormTopology);
        }
    }
}