package org.gloryjie.scheduler.core.filter;

import lombok.extern.slf4j.Slf4j;
import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.api.DagNode;
import org.gloryjie.scheduler.api.DagNodeFilter;
import org.gloryjie.scheduler.api.DagNodeInvoker;

@Slf4j
public class FullLogFilter implements DagNodeFilter {


    @Override
    public Object invoke(DagNodeInvoker dagNodeInvoker, DagNode node, DagContext dagContext) {

        try {
            log.info("[FullLogFilter] before invoke node: {}, context={}", node, dagContext);
            Object result = dagNodeInvoker.invoke(node, dagContext);
            log.info("[FullLogFilter] after invoke node: {}, context={}, result={}", node, dagContext, result);
            return result;
        } catch (Exception e) {
            log.error("[FullLogFilter] failed invoke node: {}, context={}", node, dagContext, e);
            throw e;
        }
    }

    @Override
    public int getOrder() {
        return DagNodeFilter.super.getOrder();
    }
}
