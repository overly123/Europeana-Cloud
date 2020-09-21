package eu.europeana.cloud.service.dps.storm.topologies.indexing.spout;

import eu.europeana.cloud.client.dps.rest.DpsClient;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.storm.spout.ECloudSpout;
import eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts.IndexingBolt;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class IndexingEcloudSpout extends ECloudSpout {

    private static final String AUTHORIZATION = "Authorization";

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingBolt.class);

    private final String dpsUrl;

    public IndexingEcloudSpout(String topologyName, KafkaSpoutConfig<String, DpsRecord> kafkaSpoutConfig, String hosts, int port, String keyspaceName, String userName, String password, String dpsURL) {
        super(topologyName, kafkaSpoutConfig, hosts, port, keyspaceName, userName, password);
        this.dpsUrl = dpsURL;
    }

    @Override
    protected void endTask(TaskInfo taskInfo, int processedCount) {

        DpsClient dpsClient = null;
        try {
            taskStatusUpdater.endTask(taskInfo.getId(), processedCount, TaskState.REMOVING_FROM_SOLR_AND_MONGO.toString(), TaskState.REMOVING_FROM_SOLR_AND_MONGO.toString(), new Date());
            dpsClient = new DpsClient(dpsUrl);
            DpsTask dpsTask = new ObjectMapper().readValue(taskInfo.getTaskDefinition(), DpsTask.class);
            DataSetCleanerParameters dataSetCleanerParameters = prepareCleanerParemerters(dpsTask);
            LOGGER.info("DataSet {} will be sent to be cleaned", dataSetCleanerParameters.getDataSetId());

            dpsClient.cleanMetisIndexingDataset("indexer", taskInfo.getId(), dataSetCleanerParameters,
                    AUTHORIZATION, dpsTask.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER));
            LOGGER.info("DataSet {} is sent to be cleaned and the task is finished successfully from within Storm", dataSetCleanerParameters.getDataSetId());
        } catch (Exception e) {
            LOGGER.error("An error happened while ending the task ", e);
            taskStatusUpdater.setTaskDropped(taskInfo.getId(), e.getMessage());
        } finally {
            if (dpsClient != null)
                dpsClient.close();
        }
    }

    private DataSetCleanerParameters prepareCleanerParemerters(DpsTask dpsTask) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat(IndexingBolt.DATE_FORMAT, Locale.US);
        Date recordDate = dateFormat.parse(dpsTask.getParameter(PluginParameterKeys.METIS_RECORD_DATE));
        final String useAltEnv = dpsTask.getParameter(PluginParameterKeys.METIS_USE_ALT_INDEXING_ENV);
        final String datasetId = dpsTask.getParameter(PluginParameterKeys.METIS_DATASET_ID);
        final String database = dpsTask
                .getParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE);

        return new DataSetCleanerParameters(datasetId,
                Boolean.parseBoolean(useAltEnv), database, recordDate);
    }
}
