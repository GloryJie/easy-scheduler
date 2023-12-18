package org.gloryjie.scheduler.auto;

import org.gloryjie.scheduler.api.ExecutorSelector;
import org.gloryjie.scheduler.core.SingleExcutorSelector;
import org.gloryjie.scheduler.dynamic.DefaultDynamicDagEngine;
import org.gloryjie.scheduler.dynamic.DynamicDagEngine;
import org.gloryjie.scheduler.reader.CompositeDagGraphReader;
import org.gloryjie.scheduler.reader.DagGraphFactory;
import org.gloryjie.scheduler.reader.DagGraphReader;
import org.gloryjie.scheduler.spel.SpelGraphFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.expression.BeanFactoryResolver;

@ConditionalOnProperty(prefix = "easy-scheduler", name = "enable", matchIfMissing = true, havingValue = "true")
@EnableConfigurationProperties(EasySchedulerConfig.class)
public class EasySchedulerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public DynamicDagEngine dynamicDagEngine(DagGraphFactory dagGraphFactory, ExecutorSelector executorSelector) {
        return new DefaultDynamicDagEngine(executorSelector, dagGraphFactory);
    }


    @Bean
    @ConditionalOnMissingBean
    public ExecutorSelector executorSelector() {
        return new SingleExcutorSelector(Runtime.getRuntime().availableProcessors());
    }

    @Bean
    @ConditionalOnMissingBean
    public DagGraphFactory dagGraphFactory(DagGraphReader dagGraphReader, ApplicationContext applicationContext) {
        BeanFactoryResolver beanFactoryResolver = new BeanFactoryResolver(applicationContext);
        return new SpelGraphFactory(dagGraphReader, beanFactoryResolver);
    }


    @Bean
    @ConditionalOnMissingBean
    public DagGraphReader dagGraphReader() {
        return new CompositeDagGraphReader();
    }

    @Bean
    @ConditionalOnMissingBean
    public HandlerAndGraphProcessor handlerAndGraphProcessor(EasySchedulerConfig easySchedulerConfig) {
        return new HandlerAndGraphProcessor(easySchedulerConfig);
    }

}
