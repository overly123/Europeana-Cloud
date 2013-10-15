package eu.europeana.cloud.contentserviceapi.rest;

import java.net.URI;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.contentserviceapi.exception.RecordNotExistsException;
import eu.europeana.cloud.contentserviceapi.service.RecordService;
import eu.europeana.cloud.definitions.model.Representation;

/**
 * RepresentationsResource
 */
@Path("/records/{ID}/representations")
@Component
public class RepresentationsResource {

    @Autowired
    private RecordService recordService;

    @Context
    private UriInfo uriInfo;

    @PathParam("ID")
    private String globalId;


    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public List<Representation> getRepresentations()
            throws RecordNotExistsException {
        List<Representation> representationInfos = recordService.getRecord(globalId).getRepresentations();
        prepare(representationInfos);
        return representationInfos;
    }


    private void prepare(List<Representation> representationInfos) {
        for (Representation representationInfo : representationInfos) {
            representationInfo.setFiles(null);
            EnrichUriUtil.enrich(uriInfo, representationInfo);
        }
    }
}
