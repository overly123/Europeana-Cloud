package eu.europeana.cloud.service.dps.storm.spout;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.*;
import eu.europeana.cloud.service.dps.util.LRUCache;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutMessageId;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.*;
import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;
import static org.apache.commons.collections.CollectionUtils.isEmpty;

public class ECloudSpout extends KafkaSpout<String, DpsRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ECloudSpout.class);
    private static final int MAX_RETRIES = 3;

    private String topologyName;
    private String hosts;
    private int port;
    private String keyspaceName;
    private String userName;
    private String password;

    protected transient CassandraTaskInfoDAO taskInfoDAO;
    protected transient TaskStatusUpdater taskStatusUpdater;
    protected transient TaskStatusChecker taskStatusChecker;
    protected transient ProcessedRecordsDAO processedRecordsDAO;
    private transient LRUCache<Long, TaskInfo> cache;

    public ECloudSpout(String topologyName, KafkaSpoutConfig<String, DpsRecord> kafkaSpoutConfig, String hosts, int port, String keyspaceName,
                       String userName, String password) {
        super(kafkaSpoutConfig);

        this.topologyName=topologyName;
        this.hosts = hosts;
        this.port = port;
        this.keyspaceName = keyspaceName;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public void ack(Object messageIdObject) {
        EcloudSpoutMessageId messageId = (EcloudSpoutMessageId) messageIdObject;
        LOGGER.info("Record acknowledged {}", messageId);
        //TODO if first of following lines finish with success and second not, task will never finish.
        // Scenario of this is that bolt would restart after any error in this (ack method) and after that it ignores
        // this record cause command in first of line saved record as completed.
        // Batching can be used to avoid it, but probability for such event is so small that is
        // not worth to do it.
        saveRecordCompleted(messageId);
        updateProcessedCount(messageId.getTaskId());
        super.ack(messageId.getKafkaSpoutMessageId());
    }

    private void ackIgnoredMessage(EcloudSpoutMessageId messageId) {
        super.ack(messageId.getKafkaSpoutMessageId());
    }

    private void saveRecordCompleted(EcloudSpoutMessageId messageId) {
        Retriever.retryOnError3Times("Could not save that record is completed!", () ->
                processedRecordsDAO.updateProcessedRecordState(messageId.getTaskId(), messageId.getRecordId(), RecordState.SUCCESS.toString())
        );
    }

    private void updateProcessedCount(long taskId) {
        try {
            TaskInfo taskInfo = getTaskInfoWithActualTaskSize(taskId);
            int processedCount = taskInfo.getProcessedElementCount();
            processedCount++;
            taskInfo.setProcessedElementCount(processedCount);
            LOGGER.debug("Updating processed file count task size={} processed={}", taskInfo.getExpectedSize(), taskInfo.getProcessedElementCount());
            if (processedCount == taskInfo.getExpectedSize()) {
                Retriever.retryOnError3Times("Could not save that task id=" + taskId + " is finished!", () ->
                        endTask(taskInfo, taskInfo.getProcessedElementCount())
                );
                LOGGER.info("Task {} marked as finished.", taskId);
            } else {
                Retriever.retryOnError3Times("Could not save processesed files count for taskId=" + taskId, () ->
                        taskStatusUpdater.updateProcessedFilesCount(taskId, taskInfo.getProcessedElementCount())
                );
            }

        } catch (TaskInfoDoesNotExistException e) {
            LOGGER.error("Could not found task of id=" + taskId + ", processed records counter could not be updated!");
        }

    }

    private TaskInfo getTaskInfoWithActualTaskSize(long taskId) throws TaskInfoDoesNotExistException {
        TaskInfo taskInfo = getTaskInfo(taskId);
        //task info is -1 if task is not completely sent, in such case, it must be reload from cassandra
        //to check if its size is estimated yet.
        if (taskInfo.getExpectedSize() == -1) {
            taskInfo = loadTaskFromDbToCache(taskId);
        }
        return taskInfo;
    }


    protected void endTask(TaskInfo taskInfo, int processedCount) {
        taskStatusUpdater.endTask(taskInfo.getId(), processedCount, "Completely processed", String.valueOf(TaskState.PROCESSED), new Date());
    }

    @Override
    public void fail(Object messageIdObject) {
        EcloudSpoutMessageId messageId= (EcloudSpoutMessageId) messageIdObject;
        LOGGER.error("Record failed {}", messageId);
        super.fail(messageId.getKafkaSpoutMessageId());
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, new ECloudOutputCollector(collector));
        cache = new LRUCache<>(50);
        CassandraConnectionProvider cassandraConnectionProvider =
                CassandraConnectionProviderSingleton.getCassandraConnectionProvider(
                        this.hosts,
                        this.port,
                        this.keyspaceName,
                        this.userName,
                        this.password);
        taskInfoDAO = CassandraTaskInfoDAO.getInstance(cassandraConnectionProvider);
        taskStatusUpdater = TaskStatusUpdater.getInstance(cassandraConnectionProvider);
        TaskStatusChecker.init(cassandraConnectionProvider);
        taskStatusChecker = TaskStatusChecker.getTaskStatusChecker();
        processedRecordsDAO = ProcessedRecordsDAO.getInstance(cassandraConnectionProvider);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(StormTaskTuple.getFields());
        declarer.declareStream(NOTIFICATION_STREAM_NAME, NotificationTuple.getFields());
    }

    private TaskInfo getTaskInfo(long taskId) throws TaskInfoDoesNotExistException {
        TaskInfo taskInfo = findTaskInCache(taskId);
        //
        if (taskFoundInCache(taskInfo)) {
            LOGGER.debug("TaskInfo found in cache");
        } else {
            LOGGER.debug("TaskInfo NOT found in cache");
            taskInfo = loadTaskFromDbToCache(taskId);
        }
        return taskInfo;
    }

    private TaskInfo findTaskInCache(long taskId) {
        return cache.get(taskId);
    }

    private TaskInfo loadTaskFromDbToCache(long taskId) throws TaskInfoDoesNotExistException {
        TaskInfo taskInfo;
        taskInfo = readTaskFromDB(taskId);
        cache.put(taskId, taskInfo);
        return taskInfo;
    }

    private boolean taskFoundInCache(TaskInfo taskInfo) {
        return taskInfo != null;
    }

    private TaskInfo readTaskFromDB(long taskId) throws TaskInfoDoesNotExistException {
        return Retriever.retryOnError3Times("Could not get taskInfo from db for task" + taskId, () ->
                taskInfoDAO.findById(taskId)
        ).orElseThrow(TaskInfoDoesNotExistException::new);
    }

    public class ECloudOutputCollector extends SpoutOutputCollector {

        public ECloudOutputCollector(ISpoutOutputCollector delegate) {
            super(delegate);
         }

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple, Object kafkaSpoutMessageId) {
            DpsRecord message = null;
            try {
                message = readMessageFromTuple(tuple);
                EcloudSpoutMessageId compositeMessageId = new EcloudSpoutMessageId((KafkaSpoutMessageId) kafkaSpoutMessageId, message.getTaskId(), message.getRecordId());

                if (taskStatusChecker.hasKillFlag(message.getTaskId())) {
                    return ommitMessageFromDroppedTask(message, compositeMessageId);
                }

                ProcessedRecord record = prepareRecordForExecution(message);
                if (isFinished(record)) {
                    return ommitAlreadyPerformedRecord(message, compositeMessageId);
                }

                if (maxTriesReached(record)) {
                    return emitMaxTriesReachedNotification(message, compositeMessageId);
                } else {
                    return emitRecordToPerform(streamId, message, record, compositeMessageId);
                }
            } catch (IOException | NullPointerException e) {
                LOGGER.error("Unable to read message", e);
                return Collections.emptyList();
            } catch (TaskInfoDoesNotExistException e) {
                LOGGER.error("Task definition not found in DB for: {}", message);
                return Collections.emptyList();
            }
        }

        List<Integer> ommitAlreadyPerformedRecord(DpsRecord message, EcloudSpoutMessageId messageId) {
            //Ignore records that is already preformed. It could take place after spout restart
            //if record was performed but was not acknowledged in kafka service. It is normal situation.
            //Kafka messages can be accepted in sequential order, but storm performs record in parallel so some
            //records must wait for ack before previous records will be confirmed. If spout is stopped in meantime,
            //unconfirmed but completed records would be unnecessary repeated when spout will start next time.
            LOGGER.info("Dropping kafka message for task {} because record {} was already performed: ", message.getTaskId(), message.getRecordId());
            ackIgnoredMessage(messageId);
            return Collections.emptyList();
        }

        List<Integer> emitRecordToPerform(String streamId, DpsRecord message, ProcessedRecord record, EcloudSpoutMessageId compositeMessageId) throws TaskInfoDoesNotExistException, IOException {
            TaskInfo taskInfo = getTaskInfo(message.getTaskId());
            updateRetryCount(taskInfo, record);
            StormTaskTuple stormTaskTuple = prepareTaskForEmission(taskInfo, message, record);
            LOGGER.info("Emitting record to the subsequent bolt: {}", message);
            return super.emit(streamId, stormTaskTuple.toStormTuple(), compositeMessageId);
        }

        List<Integer> emitMaxTriesReachedNotification(DpsRecord message, EcloudSpoutMessageId compositeMessageId) {
            LOGGER.info("Emitting record to the notification bolt directly because of max_retries reached: {}", message);
            NotificationTuple notificationTuple = NotificationTuple.prepareNotification(
                    message.getTaskId(),
                    message.getRecordId(),
                    RecordState.ERROR,
                    "Max retries reached",
                    "Max retries reached",
                    System.currentTimeMillis());
            return super.emit(NOTIFICATION_STREAM_NAME, notificationTuple.toStormTuple(), compositeMessageId);
        }

        List<Integer> ommitMessageFromDroppedTask(DpsRecord message, EcloudSpoutMessageId messageId) {
            // Ignores message from dropped tasks. Such message should not be emitted,
            // but must be acknowledged to not remain in topic and to allow acknowledgement
            // of any following messages.
            ackIgnoredMessage(messageId);
            LOGGER.info("Dropping kafka message because task was dropped: {}", message.getTaskId());
            return Collections.emptyList();
        }

        private boolean maxTriesReached(ProcessedRecord record) {
            return record.getAttemptNumber() > MAX_RETRIES;
        }

        private boolean isFinished(ProcessedRecord record) {
            return record.getState() == RecordState.SUCCESS || record.getState() == RecordState.ERROR;
        }

        private DpsRecord readMessageFromTuple(List<Object> tuple) {
            return (DpsRecord) tuple.get(4);
        }


        private StormTaskTuple prepareTaskForEmission(TaskInfo taskInfo, DpsRecord dpsRecord, ProcessedRecord record) throws IOException {
            //
            DpsTask dpsTask = new ObjectMapper().readValue(taskInfo.getTaskDefinition(), DpsTask.class);
            StormTaskTuple stormTaskTuple = new StormTaskTuple(
                    dpsTask.getTaskId(),
                    dpsTask.getTaskName(),
                    dpsRecord.getRecordId(),
                    null,
                    dpsTask.getParameters(),
                    dpsTask.getOutputRevision(),
                    dpsTask.getHarvestingDetails());
            //
            stormTaskTuple.addParameter(CLOUD_LOCAL_IDENTIFIER, dpsRecord.getRecordId());
            stormTaskTuple.addParameter(SCHEMA_NAME, dpsRecord.getMetadataPrefix());
            stormTaskTuple.addParameter(MESSAGE_PROCESSING_START_TIME_IN_MS, new Date().getTime() + "");

            List<String> repositoryUrlList = dpsTask.getDataEntry(InputDataType.REPOSITORY_URLS);
            if (!isEmpty(repositoryUrlList)) {
                stormTaskTuple.addParameter(DPS_TASK_INPUT_DATA, repositoryUrlList.get(0));
            }

            //Implementation of re-try mechanism after topology broken down
            stormTaskTuple.setRecordAttemptNumber(record.getAttemptNumber());

            return stormTaskTuple;
        }

        private void updateRetryCount(TaskInfo taskInfo, ProcessedRecord record) {
            if (record.getAttemptNumber() > 1) {
                LOGGER.info("Task {} record {} is repeated - {} attempt!", taskInfo.getId(), record.getRecordId(), record.getAttemptNumber());
                int retryCount = taskInfo.getRetryCount();
                retryCount++;
                taskInfo.setRetryCount(retryCount);
                taskStatusUpdater.updateRetryCount(taskInfo.getId(), retryCount);
            }
        }

        private ProcessedRecord prepareRecordForExecution(DpsRecord message) {
            ProcessedRecord record = processedRecordsDAO.selectByPrimaryKey(
                    message.getTaskId(),
                    message.getRecordId()
            ).orElseGet(() ->
                    ProcessedRecord.builder()
                            .taskId(message.getTaskId())
                            .recordId(message.getRecordId())
                            .state(RecordState.QUEUED)
                            .topologyName(topologyName)
                            .build()
            );

            record.setAttemptNumber(record.getAttemptNumber() + 1);
            processedRecordsDAO.insert(record);
            return record;
        }
    }

}
