package eu.europeana.cloud.persisted;

import eu.europeana.cloud.service.dps.storm.utils.CassandraNodeStatisticsDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraSubTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskErrorsDAO;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Created by Tarek on 4/17/2019.
 */
public class RemoverImplTest {


    @Mock(name = "subTaskInfoDAO")
    private CassandraSubTaskInfoDAO subTaskInfoDAO;


    @Mock(name = "taskErrorDAO")
    private CassandraTaskErrorsDAO taskErrorDAO;


    @Mock(name = "cassandraNodeStatisticsDAO")
    private CassandraNodeStatisticsDAO cassandraNodeStatisticsDAO;


    @InjectMocks
    private RemoverImpl removerImpl = new RemoverImpl(subTaskInfoDAO, taskErrorDAO, cassandraNodeStatisticsDAO);

    private static long TASK_ID = 1234;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this); // initialize all the @Mock objects
    }

    @Test
    public void shouldSuccessfullyRemoveNotifications() {
        doNothing().when(subTaskInfoDAO).removeNotifications(eq(TASK_ID));
        removerImpl.removeNotifications(TASK_ID);
        verify(subTaskInfoDAO, times(1)).removeNotifications((eq(TASK_ID)));
    }

    @Test(expected = Exception.class)
    public void shouldRetry5TimesBeforeFailing() {
        doThrow(Exception.class).when(subTaskInfoDAO).removeNotifications(eq(TASK_ID));
        removerImpl.removeNotifications(TASK_ID);
        verify(subTaskInfoDAO, times(6)).removeNotifications((eq(TASK_ID)));
    }


    @Test
    public void shouldSuccessfullyRemoveErrors() {
        doNothing().when(taskErrorDAO).removeErrors(eq(TASK_ID));
        removerImpl.removeErrorReports(TASK_ID);
        verify(taskErrorDAO, times(1)).removeErrors((eq(TASK_ID)));
    }

    @Test(expected = Exception.class)
    public void shouldRetry5TimesBeforeFailingWhileRemovingErrorReports() {
        doThrow(Exception.class).when(taskErrorDAO).removeErrors(eq(TASK_ID));
        removerImpl.removeErrorReports(TASK_ID);
        verify(taskErrorDAO, times(6)).removeErrors((eq(TASK_ID)));
    }

    @Test
    public void shouldSuccessfullyRemoveStatistics() {
        doNothing().when(cassandraNodeStatisticsDAO).removeStatistics(eq(TASK_ID));
        removerImpl.removeStatistics(TASK_ID);
        verify(cassandraNodeStatisticsDAO, times(1)).removeStatistics((eq(TASK_ID)));
    }

    @Test(expected = Exception.class)
    public void shouldRetry5TimesBeforeFailingWhileRemovingStatistics() {
        doThrow(Exception.class).when(cassandraNodeStatisticsDAO).removeStatistics(eq(TASK_ID));
        removerImpl.removeStatistics(TASK_ID);
        verify(cassandraNodeStatisticsDAO, times(6)).removeStatistics((eq(TASK_ID)));
    }
}