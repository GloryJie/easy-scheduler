package org.gloryjie.scheduler.reader;

import com.google.common.collect.Lists;
import org.gloryjie.scheduler.api.DagContext;

import java.util.ArrayList;
import java.util.List;

public class UserService {


    public List<String> getCourseList(DagContext dagContext) {
        return Lists.newArrayList("Math", "Java", "Go", "Rust");
    }

    public List<UserInfoContext.Course> getCourseScoreList(DagContext dagContext) {
        List<UserInfoContext.Course> courseList = new ArrayList<>();
        courseList.add(new UserInfoContext.Course("Math", 60));
        courseList.add(new UserInfoContext.Course("Java", 70));
        return courseList;
    }


    public UserInfoContext.UserInfo getUserSimpleInfoHandler(DagContext dagContext) {

        UserInfoContext.UserInfo userInfo = new UserInfoContext.UserInfo();
        userInfo.setName("Jack");
        userInfo.setAge(22);
        userInfo.setAddress("Shenzhen");

        return userInfo;
    }

}
