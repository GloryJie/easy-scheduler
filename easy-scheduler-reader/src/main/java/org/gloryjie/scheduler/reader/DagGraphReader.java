package org.gloryjie.scheduler.reader;

import java.util.List;

public interface DagGraphReader {


    /**
     * Reads a list of GraphDefinition objects from a given configuration,
     * based on the specified ConfigType.
     *
     * @param  configType The type of configuration to read from.
     * @param  content    The content of the configuration.
     * @return            A list of GraphDefinition objects read from the configuration.
     */
    List<GraphDefinition> readFromConfig(DagGraphConfigType configType, String content);

    /**
     * Reads a graph definition from the specified Java class.
     *
     * @param  clazz  the Java class to read the graph definition from
     * @return        the graph definition read from the class
     */
    GraphDefinition readFromClass(Class<?> clazz);


}
