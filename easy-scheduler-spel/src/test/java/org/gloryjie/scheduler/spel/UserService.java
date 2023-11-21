package org.gloryjie.scheduler.spel;

import com.google.common.collect.Lists;
import org.gloryjie.scheduler.reader.annotation.ContextParam;
import org.gloryjie.scheduler.reader.annotation.MethodNodeHandler;

import java.util.ArrayList;
import java.util.List;

public class UserService {


    @MethodNodeHandler("getUserCourseListHandler")
    public List<String> getCourseList(@ContextParam( "uid") Integer uid){
        System.out.println("getUserCourseListHandler param: " + uid);
        return Lists.newArrayList("Math", "Java", "Go", "Rust");
    }

    @MethodNodeHandler("getUserCourseScoreHandler")
    public List<UserInfoContext.Course> getCourseScoreList(@ContextParam( "uid") Integer uid){
        System.out.println("getUserCourseScoreHandler param: " + uid);

        List<UserInfoContext.Course> courseList = new ArrayList<>();
        courseList.add(new UserInfoContext.Course("Math", 60));
        courseList.add(new UserInfoContext.Course("Java", 70));
        return courseList;
    }


    @MethodNodeHandler("getUserSimpleInfoHandler")
    public UserInfoContext.UserInfo getUserSimpleInfoHandler(@ContextParam( "uid")Integer uid){
        System.out.println("getUserSimpleInfoHandler param: " + uid);

        UserInfoContext.UserInfo userInfo = new UserInfoContext.UserInfo();
        userInfo.setName("Jack");
        userInfo.setAge(22);
        userInfo.setAddress("Shenzhen");

        return userInfo;
    }

}
