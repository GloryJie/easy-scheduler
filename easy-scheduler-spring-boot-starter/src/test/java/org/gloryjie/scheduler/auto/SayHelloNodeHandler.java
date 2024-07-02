package org.gloryjie.scheduler.auto;

import lombok.extern.slf4j.Slf4j;
import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.api.DagNode;
import org.gloryjie.scheduler.api.NodeHandler;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SayHelloNodeHandler implements NodeHandler {


    @Override
    public String handlerName() {
        return "sayHelloNodeHandler";
    }

    @Override
    public void execute(DagNode dagNode, DagContext dagContext) {
        log.info("SayHelloNodeHandler execute, node: " + dagNode.getNodeName());
    }
}
