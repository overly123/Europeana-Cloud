package eu.europeana.cloud.service.dls.listeners;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.dls.solr.CompoundDataSetId;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/testContext.xml" })
public class AssignmentAddedListenerTest {

    @Autowired
    AssignmentAddedListener listener;

    @Autowired
    SolrDAO solrDAO;

    private static final Gson gson = new Gson();


    @After
    public void cleanUp() {
        Mockito.reset(solrDAO);
    }


    @Test
    public void shouldCallAddAssignment()
            throws Exception {
        String version = "version123123";
        String dataSet = "dataSet12";
        String provider = "provider123";
        CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
        Message message = new Message(prepareAddAssignmentMsg(version, ds).getBytes(), new MessageProperties());
        listener.onMessage(message);
        verify(solrDAO, times(1)).addAssignment(version, ds);
        verifyNoMoreInteractions(solrDAO);

    }


    @Test
    public void shouldNotCallDAOWhenReceivedEmptyMessage()
            throws Exception {

        Message message = new Message(new String("").getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }


    @Test
    public void shouldNotCallDAOMessageWithEmptyVersionProperty()
            throws Exception {

        String version = "";
        String dataSet = "dataSet";
        String provider = "provider123";
        CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
        Message message = new Message(prepareAddAssignmentMsg(version, ds).getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }


    @Test
    public void shouldNotCallDAOWhenReceivedMessageWithEmptyDataSetProperty()
            throws Exception {

        String version = "version123123";
        String dataSet = "";
        String provider = "provider123";
        CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
        Message message = new Message(prepareAddAssignmentMsg(version, ds).getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }


    @Test
    public void shouldNotCallDAOWhenReceivedMessageWithEmptyProviderProperty()
            throws Exception {

        String version = "version123123";
        String dataSet = "dataSet";
        String provider = "";
        CompoundDataSetId ds = new CompoundDataSetId(provider, dataSet);
        Message message = new Message(prepareAddAssignmentMsg(version, ds).getBytes(), new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }


    @Test
    public void shouldNotCallDAOWhenReceivedMessageWithNullBody()
            throws Exception {

        Message message = new Message(null, new MessageProperties());
        //when
        listener.onMessage(message);
        //then
        verifyZeroInteractions(solrDAO);
    }


    private String prepareAddAssignmentMsg(String versionId, CompoundDataSetId dataSetId) {
        JsonElement elem = gson.toJsonTree(dataSetId, CompoundDataSetId.class);
        JsonObject jo = new JsonObject();
        jo.add("compoundDataSetId", elem);
        jo.addProperty(ParamConstants.P_VER, versionId);
        return jo.toString();
    }
}
