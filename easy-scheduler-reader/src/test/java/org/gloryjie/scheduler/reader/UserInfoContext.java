package org.gloryjie.scheduler.reader;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.gloryjie.scheduler.reader.annotation.GraphClass;
import org.gloryjie.scheduler.reader.annotation.GraphNode;

import java.util.List;

@GraphClass(initMethod = "init", endMethod = "end")
@Data
public class UserInfoContext {

    private Integer uid;

    @GraphNode(handler = "getUserSimpleInfoHandler", dependsOn = "uid",
            paramConverter = "getUserSimpleInfoHandlerParamConverter",
            retConverter = "getUserSimpleInfoHandlerRetConverter")
    private UserInfo userInfo;


    @GraphNode(handler = "getUserCourseListHandler", dependsOn = "uid")
    private List<String> courseList;

    @GraphNode(handler = "getUserCourseScoreHandler", dependsOn = "courseList")
    private List<Course> courseScoreList;


    public void init() {
        System.out.println("init method invoke");
    }

    public void end() {
        System.out.println("end method invoke");
    }

    public Integer getUserSimpleInfoHandlerParamConverter() {
        System.out.println("param vonverter method invoke");
        return 123;
    }


    public UserInfo getUserSimpleInfoHandlerRetConverter(UserInfo userInfo) {
        System.out.println("param vonverter method invoke");
        return userInfo;
    }


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
