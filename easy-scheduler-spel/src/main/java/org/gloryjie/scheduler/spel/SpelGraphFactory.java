package org.gloryjie.scheduler.spel;

import lombok.extern.slf4j.Slf4j;
import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.reader.AbstractGraphFactory;
import org.gloryjie.scheduler.reader.DagGraphReader;
import org.springframework.expression.BeanResolver;
import org.springframework.expression.ParserContext;

import javax.annotation.Nullable;
import java.util.function.Consumer;
import java.util.function.Predicate;

@Slf4j
public class SpelGraphFactory extends AbstractGraphFactory {

    private BeanResolver beanResolver;
    private final ParserContext parserContext;

    public SpelGraphFactory() {
        super();
        parserContext = ParserContext.TEMPLATE_EXPRESSION;
    }

    public SpelGraphFactory(DagGraphReader reader) {
        super(reader);
        parserContext = ParserContext.TEMPLATE_EXPRESSION;
    }

    public SpelGraphFactory(DagGraphReader reader, BeanResolver beanResolver) {
        super(reader);
        this.beanResolver = beanResolver;
        this.parserContext = ParserContext.TEMPLATE_EXPRESSION;
    }

    public SpelGraphFactory(DagGraphReader reader, BeanResolver beanResolver, ParserContext parserContext) {
        super(reader);
        this.parserContext = parserContext;
        this.beanResolver = beanResolver;
    }

    @Nullable
    @Override
    public Predicate<DagContext> createCondition(String condition) {
        return new SpelCondition(condition, parserContext, beanResolver);
    }

    @Nullable
    @Override
    public Consumer<DagContext> createConsumer(String action) {
        return new SpelConsumer(action, parserContext, beanResolver);
    }

}
