package org.gloryjie.scheduler.reader.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.gloryjie.scheduler.reader.GraphDefinitionConfigReader;
import org.gloryjie.scheduler.reader.definition.GraphDefinition;

import java.util.List;

public class JsonGraphDefinitionReader implements GraphDefinitionConfigReader {

    private final ObjectMapper objectMapper;

    private final static TypeReference<List<GraphDefinition>> GRAPH_DEFINITION_LIST_TYPE_REF
            = new TypeReference<List<GraphDefinition>>() {};

    public JsonGraphDefinitionReader() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        this.objectMapper = mapper;
    }

    public JsonGraphDefinitionReader(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }


    @Override
    public List<GraphDefinition> read(String content) throws Exception {
        return objectMapper.readValue(content,GRAPH_DEFINITION_LIST_TYPE_REF );
    }
}
