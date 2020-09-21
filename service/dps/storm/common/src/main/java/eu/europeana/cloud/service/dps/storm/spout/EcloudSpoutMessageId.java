package eu.europeana.cloud.service.dps.storm.spout;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.apache.storm.kafka.spout.KafkaSpoutMessageId;

import java.io.Serializable;

@AllArgsConstructor
@Getter
@ToString
public class EcloudSpoutMessageId implements Serializable {

    private KafkaSpoutMessageId kafkaSpoutMessageId;

    private long taskId;

    private String recordId;

}
