package org.gloryjie.scheduler.reader;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

public class JsonGraphDefinitionReader implements GraphDefinitionReader {

    private final ObjectMapper objectMapper;

    private final static TypeReference<List<GraphDefinition>> GRAPH_DEFINITION_LIST_TYPE_REF
            = new TypeReference<List<GraphDefinition>>() {};

    public JsonGraphDefinitionReader() {
        this.objectMapper = new ObjectMapper();
    }

    public JsonGraphDefinitionReader(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }


    @Override
    public List<GraphDefinition> read(String content) throws Exception {
        return objectMapper.readValue(content,GRAPH_DEFINITION_LIST_TYPE_REF );
    }
}
