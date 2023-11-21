package org.gloryjie.scheduler.reader.data;

import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.api.NodeHandler;

public class GetUserSimpleInfoHandler implements NodeHandler<Object> {


    @Override
    public String handlerName() {
        return "getUserSimpleInfoHandler";
    }

    @Override
    public Object execute(DagContext dagContext) {

        UserInfoContext.UserInfo userInfo = new UserInfoContext.UserInfo();
        userInfo.setName("Jack");
        userInfo.setAge(22);
        userInfo.setAddress("Shenzhen");

        return userInfo;
    }
}
