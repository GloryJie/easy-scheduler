package org.gloryjie.scheduler.reader.config;

import com.google.common.collect.Lists;
import org.gloryjie.scheduler.reader.DagGraphReadException;
import org.gloryjie.scheduler.reader.GraphDefinition;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.util.ArrayList;
import java.util.List;

public class YamlGraphReader implements ConfigGraphReader {

    @Override
    public List<GraphDefinition> read(String content) {
        try {
            Constructor constructor = new Constructor(GraphDefinition.class);
            Yaml yaml = new Yaml(constructor);
            Iterable<Object> objects = yaml.loadAll(content);
            ArrayList<GraphDefinition> list = Lists.newArrayList();
            for (Object object : objects) {
                list.add((GraphDefinition) object);
            }
            return list;
        }catch (Exception e){
            throw new DagGraphReadException("Failed to read yaml config", e);
        }
    }

    @Override
    public boolean support(DagGraphConfigType configType, String content) {
        return configType == DagGraphConfigType.YAML;
    }
}
