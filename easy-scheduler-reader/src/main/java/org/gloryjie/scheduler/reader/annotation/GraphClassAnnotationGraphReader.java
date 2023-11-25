package org.gloryjie.scheduler.reader.annotation;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.gloryjie.scheduler.core.DagEngineException;
import org.gloryjie.scheduler.reader.AnnotationGraphReader;
import org.gloryjie.scheduler.reader.DagNodeDefinition;
import org.gloryjie.scheduler.reader.GraphDefinition;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

public class GraphClassAnnotationGraphReader implements AnnotationGraphReader {


    @Override
    public GraphDefinition read(Class<?> clazz) {
        GraphDefinition graphDefinition = new GraphDefinition();

        readGraphBaseInfo(graphDefinition, clazz);

        readGraphNodes(graphDefinition, clazz);

        return graphDefinition;
    }

    @Override
    public boolean support(Class<?> clazz) {
        return clazz.isAnnotationPresent(GraphClass.class);
    }

    private void readGraphBaseInfo(GraphDefinition graphDefinition, Class<?> aClass) {
        GraphClass graphClass = aClass.getAnnotation(GraphClass.class);
        if (graphClass == null) {
            throw new DagEngineException("Graph class must be annotated with @GraphClass ");
        }

        if (StringUtils.isEmpty(graphClass.graphName())) {
            graphDefinition.setGraphName(aClass.getName());
        } else {
            graphDefinition.setGraphName(graphClass.graphName());
        }
        graphDefinition.setContextClass(aClass.getName());
        graphDefinition.setTimeout(graphClass.timeout());
        graphDefinition.setNodes(new ArrayList<>());
    }

    private void readGraphNodes(GraphDefinition graphDefinition, Class<?> aClass) {
        List<Field> hadAnnotationFieldList = FieldUtils.getFieldsListWithAnnotation(aClass, GraphNode.class);

        Set<String> hadAnnotationFieldNameSet = hadAnnotationFieldList.stream()
                .map(Field::getName).collect(Collectors.toSet());

        for (Field field : hadAnnotationFieldList) {
            DagNodeDefinition nodeDefinition = new DagNodeDefinition();

            GraphNode graphNode = field.getAnnotation(GraphNode.class);
            if (StringUtils.isEmpty(graphNode.name())) {
                nodeDefinition.setNodeName(field.getName());
            } else {
                nodeDefinition.setNodeName(graphNode.name());
            }

            nodeDefinition.setRetFieldName(field.getName());
            nodeDefinition.setTimeout(graphNode.timeout());
            nodeDefinition.setHandler(graphNode.handler());
            nodeDefinition.setConditions(arrayToStrList(graphNode.conditions()));
            nodeDefinition.setActions(arrayToStrList(graphNode.actions()));


            Collection<String> fieldSet = CollectionUtils.retainAll(arrayToStrList(graphNode.dependsOn()), hadAnnotationFieldNameSet);
            nodeDefinition.setDependsOn(new HashSet<>(fieldSet));

            graphDefinition.getNodes().add(nodeDefinition);
        }

    }


    private List<String> arrayToStrList(String[] array) {
        return Arrays.stream(Optional.ofNullable(array)
                        .orElse(new String[]{}))
                .map(String::trim)
                .collect(Collectors.toList());
    }


}
