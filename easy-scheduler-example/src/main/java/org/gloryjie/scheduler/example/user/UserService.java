package org.gloryjie.scheduler.example.user;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.gloryjie.scheduler.reader.annotation.ContextParam;
import org.gloryjie.scheduler.reader.annotation.MethodNodeHandler;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class UserService {

    @MethodNodeHandler("getCourseListByUid")
    public List<Course> getCourseListByUid(@ContextParam("uid") Integer uid) {
        log.info("getUserCourseListHandler param: " + uid);
        ArrayList<Course> list = Lists.newArrayList();
        list.add(new Course(1, "Math"));
        list.add(new Course(2, "Java"));
        list.add(new Course(3, "Go"));
        list.add(new Course(4, "Rust"));
        return list;
    }

    @MethodNodeHandler("getCourseScoreList")
    public List<CourseScore> getCourseScoreList(@ContextParam("uid") Integer uid,
                                                @ContextParam("courseIdList") List<Integer> courseIdList) {
        List<CourseScore> courseList = new ArrayList<>();
        courseList.add(new CourseScore(1, 60));
        courseList.add(new CourseScore(2, 70));
        return courseList;
    }

    @MethodNodeHandler("getUserInfoByUid")
    public UserInfo getUserSimpleInfo(@ContextParam("uid") Integer uid) {
        UserInfo userInfo = new UserInfo();
        userInfo.setUid(uid);
        userInfo.setName("Jack");
        userInfo.setAge(22);
        userInfo.setAddress("Shenzhen");
        return userInfo;
    }


}
