package org.gloryjie.scheduler.reader;

import com.google.common.collect.Lists;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.util.ArrayList;
import java.util.List;

public class YamlGraphDefinitionReader implements GraphDefinitionReader {

    public YamlGraphDefinitionReader() {

    }

    @Override
    public List<GraphDefinition> read(String content) throws Exception {
        Constructor constructor = new Constructor(GraphDefinition.class);
        Yaml yaml = new Yaml(constructor);
        Iterable<Object> objects = yaml.loadAll(content);
        ArrayList<GraphDefinition> list = Lists.newArrayList();
        for (Object object : objects) {
            list.add((GraphDefinition) object);
        }
        return list;
    }
}
