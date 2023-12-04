package org.gloryjie.scheduler.reader;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefinitionValidatorTest {

    @Test
    void checkNodeDefinitionTest() {

        DagNodeDefinition nodeDefinition = new DagNodeDefinition();
        nodeDefinition.setNodeName("test");

        DefinitionValidator validator = new DefinitionValidator();

        assertThrows(DagGraphReadException.class, () -> {
            validator.checkNodeDefinition(nodeDefinition);
        });

        nodeDefinition.setHandler("test");
        assertDoesNotThrow(() -> {
            validator.checkNodeDefinition(nodeDefinition);
        });

        nodeDefinition.setHandler(null);
        nodeDefinition.setActions(Lists.newArrayList("abc"));
        assertDoesNotThrow(() -> {
            validator.checkNodeDefinition(nodeDefinition);
        });

    }

    @Test
    void checkGraphDefinitionTest() {
        GraphDefinition graphDefinition = new GraphDefinition();
        DefinitionValidator validator = new DefinitionValidator();

        assertThrows(DagGraphReadException.class, () -> {
            validator.checkGraphDefinition(graphDefinition);
        });

        graphDefinition.setGraphName("test");
        assertThrows(DagGraphReadException.class, () -> {
            validator.checkGraphDefinition(graphDefinition);
        });

        DagNodeDefinition nodeDefinition = new DagNodeDefinition();
        nodeDefinition.setNodeName("test");
        nodeDefinition.setHandler("abc");
        graphDefinition.setNodes(Lists.newArrayList(nodeDefinition));
        assertDoesNotThrow(() -> {
            validator.checkGraphDefinition(graphDefinition);
        });
    }
}