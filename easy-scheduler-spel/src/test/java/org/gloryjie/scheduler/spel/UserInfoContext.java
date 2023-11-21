package org.gloryjie.scheduler.spel;


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

    @GraphNode(handler = "getUserSimpleInfoHandler", dependsOn = "uid", conditions = "#{context.uid != null}")
    private UserInfo userInfo;


    @GraphNode(handler = "getUserCourseListHandler", dependsOn = "uid", conditions = "#{context.uid != null}")
    private List<String> courseList;

    @GraphNode(handler = "getUserCourseScoreHandler", dependsOn = "courseList", conditions = "#{context.courseList != null}")
    private List<Course> courseScoreList;


    @Data
    public static class UserInfo {

        private String name;


        private Integer age;


        private String address;

    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Course {

        private String courseName;


        private Integer score;


    }
}
