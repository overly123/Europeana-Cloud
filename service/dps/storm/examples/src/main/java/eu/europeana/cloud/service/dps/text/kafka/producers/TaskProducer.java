package eu.europeana.cloud.service.dps.text.kafka.producers;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.transform.text.pdf.PdfExtractionMethods;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class TaskProducer 
{
    public static final String datasetId = "ceffa_dataset1";
    public static final String providerId = "ceffa";

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) 
    {
        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.47.129:9093");
        props.put("serializer.class", "eu.europeana.cloud.service.dps.storm.JsonEncoder");
        props.put("request.required.acks", "1");
                
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, DpsTask> producer = new Producer<String, DpsTask>(config);

        DpsTask msg = new DpsTask();

        msg.setTaskName(PluginParameterKeys.NEW_FILE_MESSAGE);

        msg.addParameter(PluginParameterKeys.PROVIDER_ID, providerId);
        msg.addParameter(PluginParameterKeys.DATASET_ID, datasetId);
        msg.addParameter(PluginParameterKeys.EXTRACT_TEXT, "True");
        msg.addParameter(PluginParameterKeys.INDEX_DATA, "True");
        msg.addParameter(PluginParameterKeys.STORE_EXTRACTED_TEXT, "True");
        msg.addParameter(PluginParameterKeys.EXTRACTOR, PdfExtractionMethods.TIKA_EXTRACTOR.name());
        //msg.addParameter(PluginParameterKeys.FILE_URL, "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/KP2T3XNLJNJHDK3JEEVXQZEJ25QPKOLQNP4YTW4ND25V662RIQPA/representations/pdf/versions/ea3ced70-e4e6-11e4-806f-00163eefc9c8/files/test.pdf");
msg.addParameter(PluginParameterKeys.FILE_URL, "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/3WDIZUNV3TEJOHJJG7B2T54JTOBVCBF7PH55ZT7HCBEIWSTBVCLA/representations/oai/versions/a4bbd440-f4d2-11e4-9bc7-00163eefc9c8/files/meta.oai");

        KeyedMessage<String, DpsTask> data = new KeyedMessage<String, DpsTask>(
                "text_stripping", msg);
        producer.send(data);
        producer.close();
    }
    
}
