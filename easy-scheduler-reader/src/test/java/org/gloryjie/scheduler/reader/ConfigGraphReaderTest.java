package org.gloryjie.scheduler.reader;

import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.gloryjie.scheduler.reader.config.JsonGraphReader;
import org.gloryjie.scheduler.reader.config.YamlGraphReader;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


public class ConfigGraphReaderTest {




    @ParameterizedTest
    @MethodSource("fileTypeAndReaderProvider")
    public void readOneGraphTest(String fileType, ConfigGraphReader reader) throws Exception {
        File file = new File("src/test/resources/graph." + fileType);

        String cnt = FileUtils.readFileToString(file, StandardCharsets.UTF_8);

        List<GraphDefinition> result = reader.read(cnt);
        GraphDefinition graphDefinition = result.get(0);

        assertNotNull(graphDefinition.getGraphName());
        assertNotNull(graphDefinition.getTimeout());
        assertNotNull(graphDefinition.getNodes());

        DagNodeDefinition dagNodeDefinition = graphDefinition.getNodes().get(0);
        assertEquals("A",dagNodeDefinition.getNodeName());
        assertEquals("testA",dagNodeDefinition.getHandler());
        assertEquals("aField", dagNodeDefinition.getRetFieldName());
        assertEquals(Sets.newHashSet("z"), dagNodeDefinition.getDependsOn());
        assertEquals("age == 10", dagNodeDefinition.getConditions().get(0));
        assertEquals("sex == 'male'", dagNodeDefinition.getConditions().get(1));
        assertEquals("age = age + 1", dagNodeDefinition.getActions().get(0));
        assertEquals("sex = 'female'", dagNodeDefinition.getActions().get(1));
    }


    static Stream<Arguments> fileTypeAndReaderProvider() {
        return Stream.of(
                Arguments.of("json", new JsonGraphReader()),
                Arguments.of("yml", new YamlGraphReader())
        );
    }

}