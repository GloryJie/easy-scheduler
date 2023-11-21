package org.gloryjie.scheduler.reader.data;

import com.google.common.collect.Lists;
import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.api.NodeHandler;

import java.util.ArrayList;
import java.util.List;

public class GetUserCourseListHandler implements NodeHandler {



    @Override
    public String handlerName() {
        return "getUserCourseListHandler";
    }

    @Override
    public Object execute(DagContext dagContext) {
        return Lists.newArrayList("Math", "Java", "Go", "Rust");
    }
}
