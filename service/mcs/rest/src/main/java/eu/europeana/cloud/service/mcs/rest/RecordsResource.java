package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.RECORDS_RESOURCE;

/**
 * Resource representing records.
 */
@RestController
@RequestMapping(RECORDS_RESOURCE)
public class RecordsResource {

    private final RecordService recordService;

    public RecordsResource(RecordService recordService) {
        this.recordService = recordService;
    }

    /**
     * Returns record with all its latest persistent representations.
     *
     * @param cloudId cloud id of the record (required).
     * @return record.
     * @throws RecordNotExistsException provided id is not known to Unique
     * Identifier Service.
     */
    @GetMapping(produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public Record getRecord(
            HttpServletRequest httpServletRequest,
            @PathVariable String cloudId) throws RecordNotExistsException {

        Record record = recordService.getRecord(cloudId);
        prepare(httpServletRequest, record);
        return record;
    }

    /**
     * Deletes record with all its representations in all versions. Does not
     * remove mapping from Unique Identifier Service.
     *
     * <strong>Admin permissions required.</strong>
     *
     * @summary delete a record
     * @param cloudId cloud id of the record (required).
     * @throws RecordNotExistsException provided id is not known to Unique
     * Identifier Service.
     * @throws RepresentationNotExistsException thrown if no representation can
     * be found for requested record. Service cannot delete such record.
     */
    @DeleteMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @PreAuthorize("hasRole('ROLE_ADMIN')") 
    public void deleteRecord(
            @PathVariable String cloudId) throws RecordNotExistsException, RepresentationNotExistsException {

        recordService.deleteRecord(cloudId);
    }

    /**
     * Removes unimportant (at this point) information from record to reduce
     * response size.
     *
     * @param record
     */
    private void prepare(HttpServletRequest httpServletRequest, Record record) {
        EnrichUriUtil.enrich(httpServletRequest, record);
        for (Representation representation : record.getRepresentations()) {
            representation.setCloudId(null);
        }
    }
}
