package org.gloryjie.scheduler.example.user;


import lombok.Data;
import org.gloryjie.scheduler.reader.annotation.GraphClass;
import org.gloryjie.scheduler.reader.annotation.GraphNode;

import java.util.List;
import java.util.stream.Collectors;

@Data
@GraphClass
public class UserContext {

    private Integer uid;

    @GraphNode(handler = "getUserInfoByUid", dependsOn = "uid")
    private UserInfo userInfo;

    @GraphNode(handler = "getCourseListByUid", dependsOn = "uid")
    private List<Course> courseList;

    @GraphNode(handler = "getCourseScoreList", paramConverter = "uidAndCourseIdList", dependsOn = "courseList")
    private List<CourseScore> courseScoreList;


    public Object[] uidAndCourseIdList() {
        Object[] params = new Object[2];
        params[0] = uid;
        params[1] = courseList.stream().map(Course::getCourseId).collect(Collectors.toList());
        return params;
    }

}
