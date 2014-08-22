package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.Record;
import static eu.europeana.cloud.common.web.ParamConstants.F_PROVIDER;
import static eu.europeana.cloud.common.web.ParamConstants.P_CLOUDID;
import static eu.europeana.cloud.common.web.ParamConstants.P_REPRESENTATIONNAME;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;

/**
 * Resource to manage representations.
 */
@Path("/records/{" + P_CLOUDID + "}/representations/{" + P_REPRESENTATIONNAME + "}")
@Component
@Scope("request")
public class RepresentationResource {

    @Autowired
    private RecordService recordService;

    @Context
    private UriInfo uriInfo;

    @PathParam(P_CLOUDID)
    private String globalId;

    @PathParam(P_REPRESENTATIONNAME)
    private String schema;

    @Autowired
    private MutableAclService mutableAclService;

    private final String RECORD_CLASS_NAME = Record.class.getName();

    private final String REPRESENTATION_CLASS_NAME = Representation.class.getName();

    /**
     * Returns latest persistent version of representation.
     *
     * @return requested representation in latest persistent version.
     * @throws RepresentationNotExistsException representation does not exist or
     * no persistent version of this representation exists.
     */
    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Representation getRepresentation()
            throws RepresentationNotExistsException {
        Representation info = recordService.getRepresentation(globalId, schema);
        prepare(info);
        return info;
    }

    /**
     * Deletes representation with all versions.
     *
     * @throws RepresentationNotExistsException Representation does not exist.
     */
    @DELETE
    @PreAuthorize("hasPermission(#globalId.concat('/').concat(#schema), 'eu.europeana.cloud.common.model.Representation', delete)")
    public void deleteRepresentation()
            throws RepresentationNotExistsException {
        recordService.deleteRepresentation(globalId, schema);
    }

    /**
     * Creates new representation version. Url of created representation version
     * will be returned in response.
     *
     * @param providerId provider of this representation versio.
     * @return created representation.
     * @throws RecordNotExistsException provided id is not known to Unique
     * Identifier Service.
     * @throws ProviderNotExistsException no provider with given id exists
     * @statuscode 201 object has been created.
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @PreAuthorize("isAuthenticated()")
    public Response createRepresentation(@FormParam(F_PROVIDER) String providerId)
            throws RecordNotExistsException, ProviderNotExistsException {
        ParamUtil.require(F_PROVIDER, providerId);
        Representation version = recordService.createRepresentation(globalId, schema, providerId);
        EnrichUriUtil.enrich(uriInfo, version);

        String creatorName = SpringUserUtils.getUsername();
        if (creatorName != null) {
            ObjectIdentity recordIdentity = new ObjectIdentityImpl(RECORD_CLASS_NAME,
                    globalId);

            Acl exist = mutableAclService.readAclById(recordIdentity);
            if (exist == null) {
                MutableAcl recordAcl = mutableAclService.createAcl(recordIdentity);

                recordAcl.insertAce(0, BasePermission.READ, new PrincipalSid(creatorName), true);
                recordAcl.insertAce(1, BasePermission.WRITE, new PrincipalSid(creatorName), true);
                recordAcl.insertAce(2, BasePermission.DELETE, new PrincipalSid(creatorName), true);
                recordAcl.insertAce(3, BasePermission.ADMINISTRATION, new PrincipalSid(creatorName),
                        true);

                mutableAclService.updateAcl(recordAcl);
            }

            ObjectIdentity representationIdentity = new ObjectIdentityImpl(REPRESENTATION_CLASS_NAME,
                    globalId + "/" + schema);

            exist = mutableAclService.readAclById(representationIdentity);
            if (exist == null) {
                MutableAcl representationAcl = mutableAclService.createAcl(representationIdentity);

                representationAcl.insertAce(0, BasePermission.READ, new PrincipalSid(creatorName), true);
                representationAcl.insertAce(1, BasePermission.WRITE, new PrincipalSid(creatorName), true);
                representationAcl.insertAce(2, BasePermission.DELETE, new PrincipalSid(creatorName), true);
                representationAcl.insertAce(3, BasePermission.ADMINISTRATION, new PrincipalSid(creatorName),
                        true);

                mutableAclService.updateAcl(representationAcl);
            }

            ObjectIdentity versionIdentity = new ObjectIdentityImpl(REPRESENTATION_CLASS_NAME,
                    globalId + "/" + schema + "/" + version.getVersion());

            MutableAcl versionAcl = mutableAclService.createAcl(versionIdentity);

            versionAcl.insertAce(0, BasePermission.READ, new PrincipalSid(creatorName), true);
            versionAcl.insertAce(1, BasePermission.WRITE, new PrincipalSid(creatorName), true);
            versionAcl.insertAce(2, BasePermission.DELETE, new PrincipalSid(creatorName), true);
            versionAcl.insertAce(3, BasePermission.ADMINISTRATION, new PrincipalSid(creatorName),
                    true);

            mutableAclService.updateAcl(versionAcl);
        }

        return Response.created(version.getUri()).build();
    }

    private void prepare(Representation representation) {
        representation.setFiles(null);
        EnrichUriUtil.enrich(uriInfo, representation);
    }
}
