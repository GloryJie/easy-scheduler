package org.gloryjie.scheduler.example.springboot;


import org.gloryjie.scheduler.api.DagResult;
import org.gloryjie.scheduler.dynamic.DynamicDagEngine;
import org.gloryjie.scheduler.example.user.UserContext;
import org.gloryjie.scheduler.example.user.UserService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@SpringBootApplication
@Configuration
public class SpringBootApp {

    public static void main(String[] args) {
        ConfigurableApplicationContext applicationContext = SpringApplication.run(SpringBootApp.class, args);

        DynamicDagEngine dynamicDagEngine = applicationContext.getBean(DynamicDagEngine.class);

        UserContext userInfoContext = new UserContext();
        userInfoContext.setUid(123);
        DagResult dagResult = dynamicDagEngine.fireContext(userInfoContext);
        System.out.println(userInfoContext);

    }


    @Bean
    public UserService userService() {
        return new UserService();
    }

}
