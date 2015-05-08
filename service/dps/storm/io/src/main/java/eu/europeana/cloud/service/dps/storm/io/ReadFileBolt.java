package eu.europeana.cloud.service.dps.storm.io;

import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.TopologyContext;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.PersistentCountMetric;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import java.io.IOException;
import java.util.Map;

/**
 */
public class ReadFileBolt extends AbstractDpsBolt 
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ReadFileBolt.class);

    /** Properties to connect to eCloud */
    private final String zkAddress;
    private final String ecloudMcsAddress;
    private final String username;
    private final String password;

    private transient CountMetric countMetric;
    private transient PersistentCountMetric pCountMetric;
    private transient MultiCountMetric wordCountMetric;
    private transient ReducedMetric wordLengthMeanMetric;

    private FileServiceClient fileClient;

    public ReadFileBolt(String zkAddress, String ecloudMcsAddress, String username,String password) 
    {
        this.zkAddress = zkAddress;
        this.ecloudMcsAddress = ecloudMcsAddress;
        this.username = username;
        this.password = password;
    }

    @Override
    public void prepare() 
    {
        fileClient = new FileServiceClient(ecloudMcsAddress, username, password);

        initMetrics(topologyContext);
    }

    @Override
    public void execute(StormTaskTuple t) 
    {
        String fileUrl = t.getFileUrl();
 
        try
        {
            InputStream is = fileClient.getFile(fileUrl);          

            t.setFileData(is);
            
            Map<String, String> parsedUri = FileServiceClient.parseFileUri(fileUrl);
            t.addParameter(PluginParameterKeys.CLOUD_ID, parsedUri.get("CLOUDID"));
            t.addParameter(PluginParameterKeys.REPRESENTATION_NAME, parsedUri.get("REPRESENTATIONNAME"));
            t.addParameter(PluginParameterKeys.REPRESENTATION_VERSION, parsedUri.get("VERSION"));
            t.addParameter(PluginParameterKeys.FILE_NAME, parsedUri.get("FILENAME"));

            updateMetrics(t, IOUtils.toString(is));

            outputCollector.emit(inputTuple, t.toStormTuple());
            
            outputCollector.ack(inputTuple);
        }
        catch (RepresentationNotExistsException | FileNotExistsException | 
                    WrongContentRangeException ex) 
        {
            LOGGER.info("Can not retrieve file at {}", fileUrl);
            outputCollector.ack(inputTuple);
        }
        catch (DriverException | MCSException | IOException ex) 
        {
            LOGGER.error("ReadFileBolt error:" + ex.getMessage());
            outputCollector.ack(inputTuple);
        }
    }

    void initMetrics(TopologyContext context) 
    {
        countMetric = new CountMetric();
        pCountMetric = new PersistentCountMetric();
        wordCountMetric = new MultiCountMetric();
        wordLengthMeanMetric = new ReducedMetric(new MeanReducer());

        context.registerMetric("read_records=>", countMetric, 1);
        context.registerMetric("pCountMetric_records=>", pCountMetric, 1);
        context.registerMetric("word_count=>", wordCountMetric, 1);
        context.registerMetric("word_length=>", wordLengthMeanMetric, 1);
    }
    
    void updateMetrics(StormTaskTuple t, String word) 
    {
        countMetric.incr();
        pCountMetric.incr();
        wordCountMetric.scope(word).incr();
        wordLengthMeanMetric.update(word.length());
        LOGGER.info("ReadFileBolt: metrics updated");
    }
}