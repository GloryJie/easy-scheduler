package org.gloryjie.scheduler.reader.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.gloryjie.scheduler.reader.DagGraphConfigType;
import org.gloryjie.scheduler.reader.ConfigGraphReader;
import org.gloryjie.scheduler.reader.DagGraphReadException;
import org.gloryjie.scheduler.reader.GraphDefinition;

import java.util.List;

public class JsonGraphReader implements ConfigGraphReader {

    private final ObjectMapper objectMapper;

    private final static TypeReference<List<GraphDefinition>> GRAPH_DEFINITION_LIST_TYPE_REF
            = new TypeReference<List<GraphDefinition>>() {};

    public JsonGraphReader() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.objectMapper = mapper;
    }

    public JsonGraphReader(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }


    @Override
    public List<GraphDefinition> read(String content) {
        try {
            return objectMapper.readValue(content, GRAPH_DEFINITION_LIST_TYPE_REF);
        } catch (JsonProcessingException e) {
            throw new DagGraphReadException("Failed to read json config.", e);
        }
    }


    @Override
    public boolean support(DagGraphConfigType configType, String content) {
        return configType == DagGraphConfigType.JSON;
    }
}
