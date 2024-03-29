package org.gloryjie.scheduler.auto.context;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.gloryjie.scheduler.reader.annotation.GraphClass;
import org.gloryjie.scheduler.reader.annotation.GraphNode;

import java.util.List;

@GraphClass
@Data
public class UserInfoContext {

    private Integer uid;

    @GraphNode(handler = "getUserSimpleInfoHandler")
    private UserInfo userInfo;

    @GraphNode(handler = "getUserCourseListHandler")
    private List<String> courseList;

    @GraphNode(handler = "getUserCourseScoreHandler", dependsOn = "courseList")
    private List<Course> courseScoreList;

    @GraphNode(handler = "sayHelloNodeHandler")
    private String sayWord;

    @GraphNode(actions = "#{context.userInfo2 = @userService.getUserSimpleInfoHandler(context.uid)}")
    private UserInfo userInfo2;

    @Data
    public static class UserInfo {
        private String name;
        private Integer age;
        private String address;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Course {
        private String courseName;
        private Integer score;
    }
}
