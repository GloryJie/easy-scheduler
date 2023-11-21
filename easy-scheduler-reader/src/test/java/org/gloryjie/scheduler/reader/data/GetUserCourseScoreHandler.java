package org.gloryjie.scheduler.reader.data;

import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.api.NodeHandler;

import java.util.ArrayList;
import java.util.List;

public class GetUserCourseScoreHandler implements NodeHandler {



    @Override
    public String handlerName() {
        return "getUserCourseScoreHandler";
    }

    @Override
    public Object execute(DagContext dagContext) {

        List<UserInfoContext.Course> courseList = new ArrayList<>();

        courseList.add(new UserInfoContext.Course("Math", 60));
        courseList.add(new UserInfoContext.Course("Java", 70));
        return courseList;
    }
}
