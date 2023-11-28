package org.gloryjie.scheduler.reader;

import org.gloryjie.scheduler.reader.annotation.AnnotationGraphReader;
import org.gloryjie.scheduler.reader.annotation.GraphClassAnnotationGraphReader;
import org.gloryjie.scheduler.reader.config.ConfigGraphReader;
import org.gloryjie.scheduler.reader.config.DagGraphConfigType;
import org.gloryjie.scheduler.reader.config.JsonGraphReader;
import org.gloryjie.scheduler.reader.config.YamlGraphReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CompositeDagGraphReader implements DagGraphReader {

    private final List<AnnotationGraphReader> annotationGraphReaderList = new ArrayList<>();
    private final List<ConfigGraphReader> configGraphReaderList = new ArrayList<>();

    public CompositeDagGraphReader() {
        annotationGraphReaderList.add(new GraphClassAnnotationGraphReader());
        configGraphReaderList.add(new YamlGraphReader());
        configGraphReaderList.add(new JsonGraphReader());
    }

    public CompositeDagGraphReader(List<AnnotationGraphReader> annotationGraphReaderList,
                                   List<ConfigGraphReader> configGraphReaderList) {
        Objects.requireNonNull(annotationGraphReaderList, "annotationGraphReaderList cannot be null");
        Objects.requireNonNull(configGraphReaderList, "configGraphReaderList cannot be null");
        this.annotationGraphReaderList.addAll(annotationGraphReaderList);
        this.configGraphReaderList.addAll(configGraphReaderList);
    }

    @Override
    public List<GraphDefinition> readFromConfig(DagGraphConfigType configType, String configPath) {
        return configGraphReaderList.stream()
                .filter(reader -> reader.support(configType, configPath))
                .findFirst()
                .map(reader -> reader.read(configPath))
                .orElseThrow(() -> new DagGraphReadException("Could not find a supported graph reader for " +
                        "config type: " + configType));
    }

    @Override
    public GraphDefinition readFromClass(Class<?> clazz) {
        return annotationGraphReaderList.stream()
                .filter(reader -> reader.support(clazz))
                .findFirst()
                .map(reader -> reader.read(clazz))
                .orElseThrow(() -> new DagGraphReadException("Could not find a supported graph reader " +
                        "for class: " + clazz));

    }
}
