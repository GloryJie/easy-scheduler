package org.gloryjie.scheduler.reader;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertNotNull;


public class GraphDefinitionReaderTest {




    @ParameterizedTest
    @MethodSource("fileTypeAndReaderProvider")
    public void readOneGraphTest(String fileType, GraphDefinitionReader reader) throws Exception {
        File file = new File("src/test/resources/graph." + fileType);

        String cnt = FileUtils.readFileToString(file, StandardCharsets.UTF_8);

        List<GraphDefinition> result = reader.read(cnt);
        GraphDefinition graphDefinition = result.get(0);

        assertNotNull(graphDefinition.getGraphName());
        assertNotNull(graphDefinition.getTimeout());
        assertNotNull(graphDefinition.getNodes());

        DagNodeDefinition dagNodeDefinition = graphDefinition.getNodes().get(0);
        assertNotNull(dagNodeDefinition.getNodeName());
        assertNotNull(dagNodeDefinition.getTimeout());
        assertNotNull(dagNodeDefinition.getHandler());
        assertNotNull(dagNodeDefinition.getConditions());
        assertNotNull(dagNodeDefinition.getActions());
        assertNotNull(dagNodeDefinition.getDependsOn());
    }


    static Stream<Arguments> fileTypeAndReaderProvider() {
        return Stream.of(
                Arguments.of("json", new JsonGraphDefinitionReader()),
                Arguments.of("yml", new YamlGraphDefinitionReader())
        );
    }

}