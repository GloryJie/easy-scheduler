package org.gloryjie.scheduler.example.user;

import org.apache.commons.collections4.CollectionUtils;
import org.gloryjie.scheduler.api.*;
import org.gloryjie.scheduler.core.ConcurrentDagEngine;
import org.gloryjie.scheduler.core.DagGraphBuilder;
import org.gloryjie.scheduler.core.DefaultDagNode;
import org.gloryjie.scheduler.core.DefaultNodeHandler;

import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings("all")
public class BaseUseLauncher {

    public static void main(String[] args) {
        UserService userService = new UserService();

        NodeHandler userInfoHandler = DefaultNodeHandler.builder()
                .handlerName("A")
                .when(context -> context.getContext() != null)
                .action((dagNode, dagContext) -> {
                    UserContext context = (UserContext) dagContext.getContext();
                    UserInfo result = userService.getUserSimpleInfo(context.getUid());
                    context.setUserInfo(result);
                    return result;
                }).build();


        NodeHandler getCourseListHandler = DefaultNodeHandler.builder()
                .handlerName("A")
                .when(context -> context.getContext() != null)
                .action((dagNode, dagContext) -> {
                    UserContext context = (UserContext) dagContext.getContext();
                    List<Course> courseList = userService.getCourseListByUid(context.getUid());
                    context.setCourseList(courseList);
                    return courseList;
                }).build();

        NodeHandler getCourseScoreListHandler = DefaultNodeHandler.builder()
                .handlerName("A")
                .when(context -> context.getContext() != null)
                .action((dagNode, dagContext) -> {
                    UserContext context = (UserContext) dagContext.getContext();
                    if (CollectionUtils.isNotEmpty(context.getCourseList())) {
                        List<Integer> courseIdList = context.getCourseList().stream()
                                .map(Course::getCourseId).collect(Collectors.toList());
                        List<CourseScore> courseScoreList = userService.getCourseScoreList(context.getUid(), courseIdList);
                        context.setCourseScoreList(courseScoreList);
                        return courseIdList;
                    }
                    return null;
                }).build();

        DagNode<UserContext> userInfoNode = DefaultDagNode.builder()
                .nodeName("getUserInfo")
                .handler(userInfoHandler)
                .build();

        DagNode<UserContext> courseList = DefaultDagNode.builder()
                .nodeName("courseList")
                .handler(getCourseListHandler)
                .build();

        DagNode<UserContext> courseScoreList = DefaultDagNode.builder()
                .nodeName("courseScoreList")
                .handler(getCourseScoreListHandler)
                .dependOn("courseList")
                .build();

        DagGraph graph = new DagGraphBuilder().graphName("useInfoAndCourseGraph")
                .addNodes(userInfoNode, courseList, courseScoreList)
                .build();

        DagEngine dagEngine = new ConcurrentDagEngine();

        UserContext userContext = new UserContext();
        userContext.setUid(123);
        DagResult dagResult = dagEngine.fire(graph, userContext);

        if (dagResult.getState() == DagState.SUCCEED) {
            System.out.println(userContext.toString());
        } else {
            dagResult.getThrowable().printStackTrace();
        }
    }

}
