package org.gloryjie.scheduler.spel;

import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.api.NodeHandler;
import org.gloryjie.scheduler.reader.AbstractGraphFactory;
import org.gloryjie.scheduler.reader.GraphDefinitionConfigReader;
import org.springframework.expression.BeanResolver;
import org.springframework.expression.ParserContext;

import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class SpelGraphFactory extends AbstractGraphFactory {

    private ConcurrentHashMap<String, NodeHandler<Object>> handlerMap = new ConcurrentHashMap<>();


    private BeanResolver beanResolver;
    private ParserContext parserContext;


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



}
