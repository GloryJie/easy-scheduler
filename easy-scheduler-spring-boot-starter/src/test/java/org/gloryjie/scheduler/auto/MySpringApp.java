package org.gloryjie.scheduler.auto;


import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.gloryjie.scheduler.api.DagNodeFilter;
import org.gloryjie.scheduler.api.DagResult;
import org.gloryjie.scheduler.api.DagState;
import org.gloryjie.scheduler.auto.context.UserInfoContext;
import org.gloryjie.scheduler.dynamic.DynamicDagEngine;
import org.gloryjie.scheduler.resilience4j.DagNodeCircuitBreakerFilter;
import org.gloryjie.scheduler.resilience4j.HandlerCircuitBreakerFilter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Map;

@SpringBootApplication
@Configuration
public class MySpringApp {

    public static void main(String[] args) {
        ConfigurableApplicationContext applicationContext = SpringApplication.run(MySpringApp.class, args);

        Map<String, DynamicDagEngine> beansOfType = applicationContext.getBeansOfType(DynamicDagEngine.class);
        DynamicDagEngine dynamicDagEngine = new ArrayList<>(beansOfType.values()).get(0);

        UserInfoContext userInfoContext = new UserInfoContext();
        userInfoContext.setUid(123);

        DagResult dagResult = dynamicDagEngine.fireContext(userInfoContext);

        if (dagResult.getState() == DagState.SUCCEED) {
            System.out.println(userInfoContext);
        } else {
            dagResult.getThrowable().printStackTrace();
        }

    }


    @Bean
    public UserService userService() {
        return new UserService();
    }

    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry() {
        return CircuitBreakerRegistry.ofDefaults();
    }

    @Bean
    public DagNodeFilter dagNodeCircuitBreakerFilter(CircuitBreakerRegistry circuitBreakerRegistry) {
        return new DagNodeCircuitBreakerFilter(circuitBreakerRegistry);
    }

    @Bean
    public DagNodeFilter handlerCircuitBreakerFilter(CircuitBreakerRegistry circuitBreakerRegistry) {
        return new HandlerCircuitBreakerFilter(circuitBreakerRegistry);
    }

}
