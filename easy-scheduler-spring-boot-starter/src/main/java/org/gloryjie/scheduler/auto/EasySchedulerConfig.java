package org.gloryjie.scheduler.auto;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@Data
@NoArgsConstructor
@ConfigurationProperties(prefix = "easy-scheduler")
public class EasySchedulerConfig {

    private List<String> scanGraphPackage;
    private List<String> graphClass;

}



