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

import java.util.function.Predicate;

@Slf4j
public class SpelCondition implements Predicate<DagContext> {

    private final ExpressionParser parser = new SpelExpressionParser();
    private final Expression compiledExpression;
    private BeanResolver beanResolver;
    private final String expression;


    public SpelCondition(String expression, ParserContext parserContext, BeanResolver beanResolver) {
        this.expression = expression;
        compiledExpression = parser.parseExpression(expression, parserContext);
        this.beanResolver = beanResolver;
    }


    @Override
    public boolean test(DagContext dagContext) {
        try {
            StandardEvaluationContext context = new StandardEvaluationContext();
            context.setRootObject(dagContext);
            context.setVariables(dagContext.asMap());
            if (beanResolver != null) {
                context.setBeanResolver(beanResolver);
            }
            Boolean value = compiledExpression.getValue(context, Boolean.class);
            return value != null && value;
        }catch (Exception e){
            log.info("Evaluate expression: {} err, dagContext: {}", expression, dagContext);
            throw new DagEngineException("Failed to evaluate expression: " + expression, e);
        }
    }
}