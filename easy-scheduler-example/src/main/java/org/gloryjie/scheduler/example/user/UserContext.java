package org.gloryjie.scheduler.example.user;


import lombok.Data;
import org.gloryjie.scheduler.api.DependencyType;
import org.gloryjie.scheduler.reader.annotation.Dependency;
import org.gloryjie.scheduler.reader.annotation.GraphClass;
import org.gloryjie.scheduler.reader.annotation.GraphNode;

import java.util.List;
import java.util.stream.Collectors;

@Data
@GraphClass
public class UserContext {

    private Integer uid;

    @GraphNode(handler = "getUserInfoByUid", conditions = "#{context.uid != null}")
    private UserInfo userInfo;

    @GraphNode(handler = "getCourseListByUid", conditions = "#{context.uid != null}")
    private List<Course> courseList;

    @GraphNode(handler = "getCourseScoreList", paramConverter = "uidAndCourseIdList",
            dependsOn = "courseList",
            dependsOnType = {@Dependency(type = DependencyType.SOFT, on = {"userInfo"})})
    private List<CourseScore> courseScoreList;


    public Object[] uidAndCourseIdList() {
        Object[] params = new Object[2];
        params[0] = uid;
        params[1] = courseList.stream().map(Course::getCourseId).collect(Collectors.toList());
        return params;
    }

}
