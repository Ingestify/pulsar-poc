package io.ingestify.pulsar_poc.topics.common.processor;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListener;
import org.springframework.stereotype.Component;

import io.ingestify.pulsar_poc.topics.common.ListenerProcessorInitilizer;
import io.ingestify.pulsar_poc.topics.common.ScopedDeadLetterPolicy;


@Component
public class DynamicReactivePulsarListenerProcessor {
    
    @Autowired
    private ApplicationContext context;

    @Autowired
    private ReactivePulsarListenerAnnotationBeanPostProcessorAccessor accessor;

    public void processDynamicTopicListener(ListenerProcessorInitilizer annotationInitilizer, Method method, Object bean, String beanName) {
        
        annotationInitilizer.getDlp().ifPresent(dlp -> context.getBean(ScopedDeadLetterPolicy.class, dlp.getMaxRedeliverCount(), dlp.getRetryLetterTopic(), dlp.getDeadLetterTopic(), dlp.getInitialSubscriptionName()));
         
        accessor.processReactivePulsarListenerAccessor(
            new ReactivePulsarListener() {
                public Class<? extends Annotation> annotationType()          { return ReactivePulsarListener.class; }
                public String                      id()                      { return annotationInitilizer.getId(); }
                public String                      subscriptionName()        { return annotationInitilizer.getSubscriptionName(); }
                public SubscriptionType[]          subscriptionType()        { return annotationInitilizer.getSubscriptionType(); }
                public SchemaType                  schemaType()              { return annotationInitilizer.getSchemaType(); }
                public String                      containerFactory()        { return annotationInitilizer.getContainerFactory(); }
                public String[]                    topics()                  { return annotationInitilizer.getTopics(); }
                public String                      topicPattern()            { return annotationInitilizer.getTopicPattern(); }
                public String                      autoStartup()             { return annotationInitilizer.getAutoStartup(); }
                public boolean                     stream()                  { return annotationInitilizer.isStream(); }
                public String                      beanRef()                 { return annotationInitilizer.getBeanRef(); }
                public String                      concurrency()             { return annotationInitilizer.getConcurrency(); }
                public String                      useKeyOrderedProcessing() { return annotationInitilizer.getUseKeyOrderedProcessing(); }
                public String                      deadLetterPolicy()        { return annotationInitilizer.getDeadLetterPolicy(); }
                public String                      consumerCustomizer()      { return annotationInitilizer.getConsumerCustomizer(); }
            },
            method,
            bean,
            beanName);
    }
}
