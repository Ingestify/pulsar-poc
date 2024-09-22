package io.ingestify.pulsar_poc.topics.common;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.CharUtils;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.schema.SchemaType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;
import org.springframework.pulsar.listener.MessageListenerContainer;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerEndpointRegistry;

import io.ingestify.pulsar_poc.topics.common.processor.DynamicReactivePulsarListenerProcessor;
import jakarta.annotation.PreDestroy;

/**
 * A pulsar topic wrapper.
 *
 * @param <T> the type of the object to be published to the topic.
 */
public sealed abstract class ReactivePulsarTopicConsumner<T> implements Ordered permits StreamingPulsarTopic, OneByOnePulsarTopic {

    private final AtomicInteger counter = new AtomicInteger();
    
    private final ReentrantLock lock = new ReentrantLock();

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }

    @Autowired
    private DynamicReactivePulsarListenerProcessor dynamicProcessor;
    @Autowired
    private ReactivePulsarListenerEndpointRegistry<T> containerRegistry;
        
    protected record ConsumerHolder<T>(
        String listenerID,
        MessageListenerContainer consumer
    ) {}
    protected Map<String, MessageListenerContainer> listeners = new ConcurrentHashMap<>();

    public Set<String> getAllListeners() {
        return this.listeners.keySet();
    }

    /**
     * Adds a message listener to the topic.
     * @param listenerInitilizer 
     * @param containerProps 
     *
     * @param action the action to be performed when a message is received.
     * @return a UUID representing the identifier for the added message listener.
     * @throws PulsarClientException 
     */
    public String addMessageListener(
        ListenerProcessorInitilizer listenerInitilizer
        // ReactivePulsarContainerProperties<T> containerProps
    ) throws PulsarClientException {
        try {
            lock.lock();
            
            String listenerID = listenerInitilizer.getSubscriptionName() + "-" + counter.incrementAndGet();
            listenerInitilizer.setId(listenerID);
            listenerInitilizer.setBeanRef(this.getBeanName());
            listenerInitilizer.setSchemaType(this.getSchemaType());

            dynamicProcessor.processDynamicTopicListener(listenerInitilizer, getMethod(), this.getSelf(), this.getBeanName());

            MessageListenerContainer container = containerRegistry.getListenerContainer(listenerID);
            if (container.isAutoStartup()) {
                container.start();
            }

            this.listeners.put(listenerID, container);

            return listenerID;
        } finally {
            lock.unlock();
        }
    }
    
    public String getBeanName() {
        var name = this.getClass().getSimpleName().toCharArray();
        name[0] = CharUtils.toString(name[0]).toLowerCase().toCharArray()[0];
    
        return String.valueOf(name);
    }

    public Object getSelf() {
        return this;
    }
    protected abstract Method getMethod();

    protected abstract SchemaType getSchemaType();

    /**
     * Removes a message listener from the topic.
     *
     * @param listenerID the UUID representing the identifier of the listener to be removed.
     */
    public void removeMessageListener(String listenerID) {
        try {
            lock.lock();
            this.listeners.get(listenerID).destroy();
            this.listeners.remove(listenerID);
        } finally {
            lock.unlock();
        }
    }

    @PreDestroy
    public void destroy() {
        this.listeners.values().forEach(MessageListenerContainer::destroy);
    }
}
