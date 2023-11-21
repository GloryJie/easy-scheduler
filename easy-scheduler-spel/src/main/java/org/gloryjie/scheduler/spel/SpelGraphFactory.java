package org.gloryjie.scheduler.spel;

import lombok.extern.slf4j.Slf4j;
import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.reader.AbstractGraphFactory;
import org.gloryjie.scheduler.reader.annotation.MethodNodeHandler;
import org.gloryjie.scheduler.reader.config.GraphDefinitionConfigReader;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.expression.BeanResolver;
import org.springframework.expression.ParserContext;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

@Slf4j
public class SpelGraphFactory extends AbstractGraphFactory {

    private BeanResolver beanResolver;
    private final ParserContext parserContext;


    public SpelGraphFactory(GraphDefinitionConfigReader reader) {
        super(reader);
        parserContext = ParserContext.TEMPLATE_EXPRESSION;
    }

    public SpelGraphFactory(GraphDefinitionConfigReader reader, ParserContext parserContext) {
        super(reader);
        this.parserContext = parserContext;
    }

    public SpelGraphFactory(GraphDefinitionConfigReader reader, ParserContext parserContext, BeanResolver beanResolver) {
        super(reader);
        this.parserContext = parserContext;
        this.beanResolver = beanResolver;
    }

    @Nullable
    @Override
    protected Predicate<DagContext> createCondition(String condition) {
        return new SpelCondition(condition, parserContext, beanResolver);
    }

    @Nullable
    @Override
    protected Consumer<DagContext> createConsumer(String action) {
        return new SpelConsumer(action, parserContext, beanResolver);
    }


    @Override
    protected Map<Method, MethodNodeHandler> findMethodHandler(Object bean) {
        Map<Method, MethodNodeHandler> methodMap = null;

        try {
            methodMap = MethodIntrospector.selectMethods(bean.getClass(),
                    (MethodIntrospector.MetadataLookup<MethodNodeHandler>) method
                            -> AnnotatedElementUtils.findMergedAnnotation(method, MethodNodeHandler.class));
        } catch (Exception e) {
            // ignore
            log.debug("Failed to find method handler", e);
        }

        return methodMap;
    }
}
