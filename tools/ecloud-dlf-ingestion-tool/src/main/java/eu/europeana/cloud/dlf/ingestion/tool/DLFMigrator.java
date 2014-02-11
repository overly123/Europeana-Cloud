package eu.europeana.cloud.dlf.ingestion.tool;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationServiceClient;
import eu.europeana.cloud.mcs.driver.SearchParams;
import eu.europeana.cloud.mcs.driver.SearchServiceClient;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class DLFMigrator {

    String uisUrl = "";
    String mcsUrl = "";
    String providerId = "providerId";

    private final UISClient uisClient;
    private final RepresentationServiceClient representationClient;
    private final FileServiceClient filesClient;
    private final DataSetServiceClient dataSetClient;
    private final SearchServiceClient searchClient;


    public DLFMigrator() {
        uisClient = new UISClient(uisUrl);
        representationClient = new RepresentationServiceClient();
        filesClient = new FileServiceClient();
        dataSetClient = new DataSetServiceClient(mcsUrl);
        searchClient = new SearchServiceClient();
    }


    public void migrate() {
        try {

            //create or get provider

            uisClient.createProvider(providerId, null);

            List<File> filesList = new ArrayList<>();
            for (File f : filesList) {
                migrateRecord(f);
            }

        } catch (CloudException ex) {

        }

    }


    /**
     * 
     * @param f
     *            folder containing various record representations. Name of the folder is record local id.
     */
    private void migrateRecord(File f)
            throws CloudException {
        //read fbcId (localId)
        String localId = "";
        CloudId cloudId = uisClient.createCloudId(providerId, localId);

        //get list of files
        //for all files in dir
        migrateFile(cloudId.getId());

        createDataSet();

    }


    private void migrateFile(String cloudId) {
        //        String versionId = 
        representationClient.createRepresentation();

        //add file to representation 
        InputStream data = null;
        filesClient.uploadFile("cloudId", "schema", "version", data);

        //persist representation
        representationClient.persistVersion("cloudId", "schemaId", "versionId");
    }


    private void createDataSet() {
        try {
            dataSetClient.createDataSet("dataSetId", providerId, "description");
            //for all created files
            dataSetClient.assignRepresentationToDataSet("dataSetId", "providerId", "cloudId", "schemaId", "versionId");
        } catch (DataSetAlreadyExistsException ex) {

        } catch (ProviderNotExistsException ex) {
        }
    }


    public void searchImportedRecords() {
        searchClient.search(new SearchParams());

    }
}
