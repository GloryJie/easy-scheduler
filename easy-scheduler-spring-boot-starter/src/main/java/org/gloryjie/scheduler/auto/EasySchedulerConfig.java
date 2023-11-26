package org.gloryjie.scheduler.auto;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@NoArgsConstructor
@ConfigurationProperties(prefix = "easy-scheduler")
@Configuration
public class EasySchedulerConfig {

    private String base;
    private Boolean enable;
    private List<String> scanGraphPackage;
    private List<String> graphClass;

}



