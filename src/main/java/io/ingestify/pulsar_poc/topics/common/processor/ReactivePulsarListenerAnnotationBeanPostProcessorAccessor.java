package io.ingestify.pulsar_poc.topics.common.processor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListener;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListenerAnnotationBeanPostProcessor;
import org.springframework.stereotype.Component;

@Component
class ReactivePulsarListenerAnnotationBeanPostProcessorAccessor {
    
    @Autowired
    private ApplicationContext context;

    void processReactivePulsarListenerAccessor(ReactivePulsarListener reactivePulsarListener, Method method,
			Object bean, String beanName) {
        
        try {
            Method processReactivePulsarListener = ReactivePulsarListenerAnnotationBeanPostProcessor.class.getDeclaredMethod("processReactivePulsarListener", ReactivePulsarListener.class, Method.class, Object.class, String.class);
            
            var processor = context.getBean(ReactivePulsarListenerAnnotationBeanPostProcessor.class);

            processReactivePulsarListener.setAccessible(true);
            processReactivePulsarListener.invoke(processor, reactivePulsarListener, method, bean, beanName);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            e.printStackTrace();
        }
    }
}
