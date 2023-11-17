package org.gloryjie.scheduler.spel;

import lombok.extern.slf4j.Slf4j;
import org.gloryjie.scheduler.api.DagContext;
import org.gloryjie.scheduler.core.DagEngineException;
import org.springframework.expression.BeanResolver;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.util.function.Consumer;

@Slf4j
public class SpelConsumer implements Consumer<DagContext> {

    private final ExpressionParser parser = new SpelExpressionParser();
    private final Expression compiledExpression;
    private final BeanResolver beanResolver;
    private final String expression;


    public SpelConsumer(String expression, ParserContext parserContext, BeanResolver beanResolver) {
        this.expression = expression;
        compiledExpression = parser.parseExpression(expression, parserContext);
        this.beanResolver = beanResolver;
    }


    @Override
    public void accept(DagContext dagContext) {
        try {
            StandardEvaluationContext context = new StandardEvaluationContext();
            context.setRootObject(dagContext);
            context.setVariables(dagContext.asMap());
            if (beanResolver != null) {
                context.setBeanResolver(beanResolver);
            }
            // consumer not return value
            compiledExpression.getValue(context);
        } catch (Exception e) {
            log.info("Execute expression: {} err, dagContext: {}", expression, dagContext);
            throw new DagEngineException("Failed to Execute expression: " + expression, e);
        }
    }

}
