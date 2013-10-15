package eu.europeana.cloud.uidservice.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import eu.europeana.cloud.definitions.model.GlobalId;
import eu.europeana.cloud.definitions.model.Provider;
import eu.europeana.cloud.definitions.model.Record;
import eu.europeana.cloud.definitions.model.Representation;
import eu.europeana.cloud.exceptions.DatabaseConnectionException;
import eu.europeana.cloud.exceptions.GlobalIdDoesNotExistException;
import eu.europeana.cloud.exceptions.IdHasBeenMappedException;
import eu.europeana.cloud.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordDatasetEmptyException;
import eu.europeana.cloud.exceptions.RecordDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordExistsException;
import eu.europeana.cloud.exceptions.RecordIdDoesNotExistException;

@Service
public class UniqueIdServiceImpl implements UniqueIdService {

	private List<Record> records = new ArrayList<>();
	private Map<String, List<String>> providerLocalIds = new HashMap<>();
	private Map<String, List<String>> providerGlobalIds = new HashMap<>();
 	@Override
	public GlobalId create(String providerId, String recordId)
			throws DatabaseConnectionException, RecordExistsException {
		String globalId = String.format("/%s/%s", providerId, recordId);
		for (Record record : records) {
			if (StringUtils.equals(record.getId(), globalId)) {
				throw new RecordExistsException();
			}
		}
		Record record = new Record();
		Representation rep = new Representation();
		rep.setDataProvider(providerId);
		rep.setRecordId(recordId);
		List<Representation> reps = new ArrayList<>();
		reps.add(rep);
		record.setRepresentations(reps);
		record.setId(globalId);
		records.add(record);
		List<String> recordList = providerLocalIds.get(providerId) != null ? providerLocalIds
				.get(providerId) : new ArrayList<String>();
		if (!recordList.contains(recordId)) {
			recordList.add(recordId);
		}
		providerLocalIds.put(providerId, recordList);
		
		List<String> globalList = providerGlobalIds.get(providerId) != null ? providerGlobalIds
				.get(providerId) : new ArrayList<String>();
		if (!globalList.contains(globalId)) {
			globalList.add(globalId);
		}
		providerGlobalIds.put(providerId, globalList);
		
		Provider provider = new Provider();
		provider.setId(providerId);
		provider.setRecordId(recordId);
		
		GlobalId gId = new GlobalId();
		gId.setProvider(provider);
		gId.setId(globalId);
		return gId;
	}

	@Override
	public GlobalId search(String providerId, String recordId)
			throws DatabaseConnectionException, RecordDoesNotExistException {
		for (Record rec : records) {

			for (Representation representation : rec.getRepresentations()) {
				if (StringUtils.equals(representation.getDataProvider(),
						providerId)
						&& StringUtils.equals(representation.getRecordId(),
								recordId)) {
					Provider provider = new Provider();
					provider.setId(providerId);
					provider.setRecordId(recordId);
					
					GlobalId gId = new GlobalId();
					gId.setProvider(provider);
					gId.setId(rec.getId());
					return gId;
				}
			}
		}
		throw new RecordDoesNotExistException();
	}

	@Override
	public List<Provider> searchByGlobalId(String globalId)
			throws DatabaseConnectionException, GlobalIdDoesNotExistException {
		for (Record record : records) {
			if (StringUtils.equals(record.getId(),globalId)) {
				List<Provider> providers = new ArrayList<>();
				
				for (Representation rep : record.getRepresentations()) {
					Provider provider = new Provider();
					provider.setId(rep.getDataProvider());
					provider.setRecordId(rep.getRecordId());
					providers.add(provider);
				}
				return providers;
			}
		}
		throw new GlobalIdDoesNotExistException();
	}

	@Override
	public List<Provider> searchLocalIdsByProvider(String providerId, int start,
			int end) throws DatabaseConnectionException,
			ProviderDoesNotExistException, RecordDatasetEmptyException {
		if (providerLocalIds.containsKey(providerId)) {
			if (providerLocalIds.get(providerId).isEmpty()
					|| providerLocalIds.get(providerId).size() < start) {
				throw new RecordDatasetEmptyException();
			}
			List<Provider> providers = new ArrayList<>();
			for (String localId : providerLocalIds.get(providerId).subList(
					start,
					Math.min(providerLocalIds.get(providerId).size(), start
							+ end))){
				Provider provider = new Provider();
				provider.setId(providerId);
				provider.setRecordId(localId);
				providers.add(provider);
			}
			return providers;
		}
		throw new ProviderDoesNotExistException();
	}

	@Override
	public List<GlobalId> searchGlobalIdsByProvider(String providerId, int start,
			int end) throws DatabaseConnectionException,
			ProviderDoesNotExistException, RecordDatasetEmptyException {
		if (providerGlobalIds.containsKey(providerId)) {
			if (providerLocalIds.get(providerId).isEmpty()
					|| providerLocalIds.get(providerId).size() < start) {
				throw new RecordDatasetEmptyException();
			}

			List<GlobalId> globalIds = new ArrayList<>();
			for (String globalId : providerGlobalIds.get(providerId).subList(
					start,
					Math.min(providerGlobalIds.get(providerId).size(), start
							+ end))){
				Provider provider = new Provider();
				provider.setId(providerId);
				
				GlobalId gId = new GlobalId();
				gId.setProvider(provider);
				gId.setId(globalId);
				globalIds.add(gId);
			}
			return globalIds;
			
		}
		throw new ProviderDoesNotExistException();
	}

	@Override
	public void createFromExisting(String globalId, String providerId,
			String recordId) throws DatabaseConnectionException,
			ProviderDoesNotExistException, GlobalIdDoesNotExistException,
			RecordIdDoesNotExistException, IdHasBeenMappedException {
		if(!providerLocalIds.containsKey(providerId)){
			throw new ProviderDoesNotExistException();
		}
		if(!providerLocalIds.get(providerId).contains(recordId)){
			throw new RecordIdDoesNotExistException();
		}
		if(providerGlobalIds.get(providerId).contains(globalId)){
			throw new IdHasBeenMappedException();
		}
		for(Record record:records){
			if(StringUtils.equals(record.getId(), globalId)){
				Representation rep = new Representation();
				rep.setRecordId(recordId);
				rep.setDataProvider(providerId);
				record.getRepresentations().add(rep);
			}
		}
		throw new GlobalIdDoesNotExistException();
	}

	@Override
	public void removeMappingByLocalId(String providerId, String recordId)
			throws DatabaseConnectionException, ProviderDoesNotExistException,
			RecordIdDoesNotExistException {
		// Mockup it will be soft delete in reality
		for(Record record:records){
			for (Representation representation:record.getRepresentations()){
				if(StringUtils.equals(representation.getDataProvider(),providerId)&&
						StringUtils.equals(recordId, representation.getRecordId())){
					records.remove(record);
				}
			}
		}

	}

	@Override
	public void deleteGlobalId(String globalId)
			throws DatabaseConnectionException, GlobalIdDoesNotExistException {
		// Mockup it will be soft delete in reality
		for (Record record : records) {
			if (StringUtils.equals(record.getId(), globalId)) {
				records.remove(record);
			}
		}
		throw new GlobalIdDoesNotExistException();
	}
}
