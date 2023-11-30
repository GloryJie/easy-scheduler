package org.gloryjie.scheduler.auto;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.gloryjie.scheduler.api.NodeHandler;
import org.gloryjie.scheduler.core.DagEngineException;
import org.gloryjie.scheduler.dynamic.DynamicDagEngine;
import org.gloryjie.scheduler.reader.annotation.GraphClass;
import org.gloryjie.scheduler.reader.annotation.MethodNodeHandler;
import org.gloryjie.scheduler.reader.annotation.MethodNodeHandlerImpl;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class HandlerAndGraphProcessor implements SmartInitializingSingleton,
        ApplicationContextAware {

    private ApplicationContext applicationContext;

    private List<DynamicDagEngine> dynamicDagEngineList;

    private EasySchedulerConfig easySchedulerConfig;

    public HandlerAndGraphProcessor(EasySchedulerConfig easySchedulerConfig) {
        this.easySchedulerConfig = easySchedulerConfig;
    }

    @Override
    public void afterSingletonsInstantiated() {
        if (CollectionUtils.isEmpty(dynamicDagEngineList)) {
            return;
        }
        // Register all impl node handlers
        registerImplNodeHandlerBean();

        // Register all method node handlers
        registerMethodNodeHandler();

        // Register all annotation graphs
        registerAnnotationGraph();

    }

    private void registerImplNodeHandlerBean() {
        Map<String, NodeHandler> beansOfType = applicationContext.getBeansOfType(NodeHandler.class, false, false);
        for (NodeHandler nodeHandler : beansOfType.values()) {
            for (DynamicDagEngine dynamicDagEngine : dynamicDagEngineList) {
                dynamicDagEngine.registerHandler(nodeHandler);
            }
            log.info("[Easy-Scheduler]register impl node handler: {}", nodeHandler.handlerName());
        }
    }

    private void registerAnnotationGraph() {
        EasySchedulerConfig configProperties = easySchedulerConfig;

        if (CollectionUtils.isNotEmpty(configProperties.getGraphClass())) {
            for (String className : configProperties.getGraphClass()) {
                registerGraphByClassName(className);
            }
        }

        if (CollectionUtils.isNotEmpty(configProperties.getScanGraphPackage())) {
            ClassPathScanningCandidateComponentProvider scanner =
                    new ClassPathScanningCandidateComponentProvider(false);
            scanner.addIncludeFilter(new AnnotationTypeFilter(GraphClass.class));

            List<String> packages = configProperties.getScanGraphPackage();
            for (String basePackage : packages) {

                Set<BeanDefinition> candidateComponents = scanner.findCandidateComponents(basePackage);
                for (BeanDefinition candidateComponent : candidateComponents) {
                    String beanClassName = candidateComponent.getBeanClassName();
                    registerGraphByClassName(beanClassName);
                }
            }
        }
    }

    private void registerGraphByClassName(String className) {
        Class<?> clazz = null;
        try {
            clazz = ClassUtils.forName(className, this.applicationContext.getClassLoader());
        } catch (Exception e) {
            throw new DagEngineException("Failed to load graph class[" + className + "]", e);
        }
        for (DynamicDagEngine dynamicDagEngine : dynamicDagEngineList) {
            dynamicDagEngine.registerGraphClass(clazz);
        }
        log.info("[Easy-Scheduler] register graph class: {} ", clazz.getName());
    }

    private void registerMethodNodeHandler() {
        ApplicationContext beanFactory = this.applicationContext;
        String[] beanNames = beanFactory.getBeanNamesForType(Object.class);
        for (String beanName : beanNames) {
            Object bean = beanFactory.getBean(beanName);
            processBean(bean, bean.getClass());
        }
    }

    private void processBean(Object target, Class<?> clazz) {
        if (AnnotationUtils.isCandidateClass(clazz, MethodNodeHandler.class)) {
            Map<Method, MethodNodeHandler> methodMap = null;

            try {
                // use spring MethodIntrospector to find true method
                methodMap = MethodIntrospector.selectMethods(clazz,
                        (MethodIntrospector.MetadataLookup<MethodNodeHandler>) method
                                -> AnnotatedElementUtils.findMergedAnnotation(method, MethodNodeHandler.class));
            } catch (Exception e) {
                // ignore
                log.debug("Failed to find method handler", e);
            }

            if (MapUtils.isEmpty(methodMap)) {
                return;
            }

            for (Map.Entry<Method, MethodNodeHandler> entry : methodMap.entrySet()) {
                Method method = entry.getKey();
                MethodNodeHandler annotation = entry.getValue();
                MethodNodeHandlerImpl handler = new MethodNodeHandlerImpl(target, method, annotation);
                for (DynamicDagEngine dynamicDagEngine : dynamicDagEngineList) {
                    dynamicDagEngine.registerHandler(handler);
                }
                log.info("[Easy-Scheduler]register method handler: {}", handler.handlerName());
            }

        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        Map<String, DynamicDagEngine> beansOfType = applicationContext.getBeansOfType(DynamicDagEngine.class, false, false);
        this.dynamicDagEngineList = new ArrayList<>(beansOfType.values());
    }
}
