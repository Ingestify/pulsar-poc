package io.ingestify.pulsar_poc.topics.common;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.schema.SchemaType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.MethodParameter;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.ConditionalGenericConverter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.PayloadMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.pulsar.annotation.AbstractPulsarAnnotationsBeanPostProcessor;
import org.springframework.pulsar.config.GenericListenerEndpointRegistry;
import org.springframework.pulsar.listener.MessageListenerContainer;
import org.springframework.pulsar.reactive.config.MethodReactivePulsarListenerEndpoint;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerEndpoint;
import org.springframework.pulsar.reactive.core.ReactivePulsarTemplate;
import org.springframework.pulsar.reactive.listener.ReactivePulsarContainerProperties;
import org.springframework.pulsar.reactive.listener.ReactivePulsarMessageListenerContainer;
import org.springframework.pulsar.support.PulsarNull;
import org.springframework.util.Assert;

import jakarta.annotation.PreDestroy;

/**
 * A pulsar topic wrapper.
 *
 * @param <T> the type of the object to be published to the topic.
 */
public sealed abstract class ReactivePulsarTopic<T> extends AbstractPulsarAnnotationsBeanPostProcessor permits StreamingPulsarTopic, OneByOnePulsarTopic {

    private final AtomicInteger counter = new AtomicInteger();
    
    private final ReentrantLock lock = new ReentrantLock();

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }

    @Autowired
    ReactiveConsumerFactory<T> factory;

    @Autowired
    // @SuppressWarnings("rawtypes")
    // private GenericListenerEndpointRegistry endpointRegistry;
    private GenericListenerEndpointRegistry<ReactivePulsarMessageListenerContainer<T>, ReactivePulsarListenerEndpoint<T>> 
        endpointRegistry;
        
    @Autowired
    private ReactivePulsarTemplate<T> template;

    protected record ConsumerHolder<T>(
        String listenerID,
        MessageListenerContainer consumer
    ) {}
    protected Map<String, MessageListenerContainer> listeners = new ConcurrentHashMap<>();
    // protected Many<Message<T>> topic = Sinks.many().multicast().<Message<T>>onBackpressureBuffer();
    // protected Many<T> topic = Sinks.many().multicast().

    /**
     * Returns the name of the topic.
     *
     * @return the name of the topic as a String.
     */
    public abstract String getTopicName();

    /**
     * Publishes an object to the topic.
     *
     * @param object the object to be published.
     */
    public void publish(T object) {
            
        template.send(getTopicName(), object).block();
    }

    public Set<String> getAllListeners() {
        return this.listeners.keySet();
    }

    /**
     * Adds a message listener to the topic.
     * @param subscriptionData 
     * @param containerProps 
     *
     * @param action the action to be performed when a message is received.
     * @return a UUID representing the identifier for the added message listener.
     * @throws PulsarClientException 
     */
    public String addMessageListener(
        SubscriptionData subscriptionData,
        ReactivePulsarContainerProperties<T> containerProps
    ) throws PulsarClientException {
        try {
            lock.lock();
            
            ReactivePulsarListenerContainerFactory<T> listenerfactory 
                = factory.createConsumerFactory(getTopicName(), subscriptionData, containerProps);

            MethodReactivePulsarListenerEndpoint<T> ep = new MethodReactivePulsarListenerEndpoint<>();
            ep.setBean(this);
            ep.setMethod(getMethod());
            String listenerID = getTopicName() + "-" + counter.incrementAndGet();
            ep.setId(listenerID);
            ep.setAutoStartup(false);
            
            ep.setMessageHandlerMethodFactory(messageHandlerMethodFactory);
            ep.setMessageHandlerMethodFactory(new DefaultMessageHandlerMethodFactory());
            
            // ep.setSchemaType(Schema.JSON(getClazz()));
            ep.setSchemaType(getSchemaType());
            

            endpointRegistry.registerListenerContainer(ep, listenerfactory);

            MessageListenerContainer container = endpointRegistry.getListenerContainer(listenerID);
            
            // This might be needed to start the listener
            container.start();

            this.listeners.put(listenerID, container);

            return listenerID;
        } finally {
            lock.unlock();
        }
    }

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

    protected abstract Method getMethod();
    protected abstract Class<T> getClazz();

    private Charset charset = StandardCharsets.UTF_8;

    protected class PulsarHandlerMethodFactoryAdapter implements MessageHandlerMethodFactory {

		protected final DefaultFormattingConversionService defaultFormattingConversionService = new DefaultFormattingConversionService();

		private MessageHandlerMethodFactory handlerMethodFactory;

		public void setHandlerMethodFactory(MessageHandlerMethodFactory pulsarHandlerMethodFactory1) {
			this.handlerMethodFactory = pulsarHandlerMethodFactory1;
		}

		@Override
		public InvocableHandlerMethod createInvocableHandlerMethod(Object bean, Method method) {
			return getHandlerMethodFactory().createInvocableHandlerMethod(bean, method);
		}

		private MessageHandlerMethodFactory getHandlerMethodFactory() {
			if (this.handlerMethodFactory == null) {
				this.handlerMethodFactory = createDefaultMessageHandlerMethodFactory();
			}
			return this.handlerMethodFactory;
		}

		private MessageHandlerMethodFactory createDefaultMessageHandlerMethodFactory() {
			DefaultMessageHandlerMethodFactory defaultFactory = new DefaultMessageHandlerMethodFactory();
			defaultFactory.setBeanFactory(ReactivePulsarTopic.this.beanFactory);
			this.defaultFormattingConversionService
				.addConverter(new BytesToStringConverter(ReactivePulsarTopic.this.charset));
			this.defaultFormattingConversionService.addConverter(new BytesToNumberConverter());
			defaultFactory.setConversionService(this.defaultFormattingConversionService);
			GenericMessageConverter messageConverter = new GenericMessageConverter(
					this.defaultFormattingConversionService);
			defaultFactory.setMessageConverter(messageConverter);
			defaultFactory
				.setCustomArgumentResolvers(List.of(new PulsarNullAwarePayloadArgumentResolver(messageConverter)));
			defaultFactory.afterPropertiesSet();
			return defaultFactory;
		}

		public DefaultFormattingConversionService getDefaultFormattingConversionService() {
			return this.defaultFormattingConversionService;
		}

	}

    public class PulsarNullAwarePayloadArgumentResolver extends PayloadMethodArgumentResolver {

        PulsarNullAwarePayloadArgumentResolver(MessageConverter messageConverter) {
            super(messageConverter);
        }

        @Override
        public Object resolveArgument(MethodParameter parameter, Message<?> message) throws Exception {
            if (message == null) {
                message = new GenericMessage<>(PulsarNull.INSTANCE);
            }
            Object resolved = super.resolveArgument(parameter, message);
            // Replace 'PulsarNull' elements w/ 'null'
            if (resolved instanceof List<?> list) {
                for (int i = 0; i < list.size(); i++) {
                    if (list.get(i) instanceof PulsarNull) {
                        list.set(i, null);
                    }
                }
            }
            return resolved;
        }

        @Override
        protected boolean isEmptyPayload(Object payload) {
            return payload == null || payload instanceof PulsarNull;
        }

    }

    private static class BytesToStringConverter implements Converter<byte[], String> {

		private final Charset charset;

		BytesToStringConverter(Charset charset) {
			this.charset = charset;
		}

		@Override
		public String convert(byte[] source) {
			return new String(source, this.charset);
		}

	}

	private final class BytesToNumberConverter implements ConditionalGenericConverter {

		BytesToNumberConverter() {
		}

		@Override
		@Nullable
		public Set<ConvertiblePair> getConvertibleTypes() {
			HashSet<ConvertiblePair> pairs = new HashSet<>();
			pairs.add(new ConvertiblePair(byte[].class, long.class));
			pairs.add(new ConvertiblePair(byte[].class, int.class));
			pairs.add(new ConvertiblePair(byte[].class, short.class));
			pairs.add(new ConvertiblePair(byte[].class, byte.class));
			pairs.add(new ConvertiblePair(byte[].class, Long.class));
			pairs.add(new ConvertiblePair(byte[].class, Integer.class));
			pairs.add(new ConvertiblePair(byte[].class, Short.class));
			pairs.add(new ConvertiblePair(byte[].class, Byte.class));
			return pairs;
		}

		@Override
		@Nullable
		public Object convert(@Nullable Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {
			byte[] bytes = (byte[]) source;
			if (targetType.getType().equals(long.class) || targetType.getType().equals(Long.class)) {
				Assert.state(bytes.length >= 8, "At least 8 bytes needed to convert a byte[] to a long"); // NOSONAR
				return ByteBuffer.wrap(bytes).getLong();
			}
			else if (targetType.getType().equals(int.class) || targetType.getType().equals(Integer.class)) {
				Assert.state(bytes.length >= 4, "At least 4 bytes needed to convert a byte[] to an integer"); // NOSONAR
				return ByteBuffer.wrap(bytes).getInt();
			}
			else if (targetType.getType().equals(short.class) || targetType.getType().equals(Short.class)) {
				Assert.state(bytes.length >= 2, "At least 2 bytes needed to convert a byte[] to a short");
				return ByteBuffer.wrap(bytes).getShort();
			}
			else if (targetType.getType().equals(byte.class) || targetType.getType().equals(Byte.class)) {
				Assert.state(bytes.length >= 1, "At least 1 byte needed to convert a byte[] to a byte");
				return ByteBuffer.wrap(bytes).get();
			}
			return null;
		}

		@Override
		public boolean matches(TypeDescriptor sourceType, TypeDescriptor targetType) {
			if (sourceType.getType().equals(byte[].class)) {
				Class<?> target = targetType.getType();
				return target.equals(long.class) || target.equals(int.class) || target.equals(short.class) // NOSONAR
						|| target.equals(byte.class) || target.equals(Long.class) || target.equals(Integer.class)
						|| target.equals(Short.class) || target.equals(Byte.class);
			}
			return false;
		}

	}

    /*
                    // BiConsumer<Consumer<T>, Message<T>> action, Class<T> clazz) throws PulsarClientException {
        try {
            lock.lock();
            
            // MessageListener<T> listener = new MessageListener<T>() {
            //     @Override
            //     public void received(Consumer<T> consumer, Message<T> message) {
            //         var res = topic.tryEmitNext(message);
            //         if (res.isSuccess()) {

            //         }
            //         action.accept(consumer, message);
            //     }
            // };

            // reactivePulsarListenerFactory = ReactivePulsarListenerContainerFactory.
            //     .
            
            //     .createContainerInstance(new AbstractReactivePulsarListenerEndpoint<T>() {

            //         @Override
            //         protected AbstractPulsarMessageToSpringMessageAdapter<T> createMessageHandler(
            //                 ReactivePulsarMessageListenerContainer<T> container, MessageConverter messageConverter) {
            //             messageConverter.
            //         }

            //     })
            //     .start();

            // ReactiveMessageConsumer<T> reactiveConsumer = consumerFactory.createConsumer(Schema.JSON(clazz));
            // ReactiveMessagePipeline pipe = reactiveConsumer.messagePipeline()
            //     .streamingMessageHandler((messages) -> {
            //         return messages.map(m -> {
            //             return MessageResult.acknowledge(m);
            //         });
            //     })
            //     .build();

            // pipe.start();
            
            // Consumer<T> consumer = pulsarClient.newConsumer(Schema.JSON(clazz))
            //     .topic(subscriptionDto.topic())
            //     .subscriptionName(subscriptionDto.subscriptionName())
            //     .subscriptionType(subscriptionDto.subscriptionType())
            //     .isAckReceiptEnabled(true)
            //     .messageListener(listener)
            //     .subscribe();
            

            // var listenerDisposable = topic.asFlux().subscribe(c -> {
            //     // c.
            // });

            // ReactivePulsarConsumerFactory<T> consumerFactory =
            //     new DefaultReactivePulsarConsumerFactory<T>(
            //         reactivePulsarClient,
            //         List.of(
            //             (c) -> c.topic(subscriptionDto.topic())
            //                     .consumerName(subscriptionDto.subscriptionName + "-consumer")
            //                     .subscriptionName(subscriptionDto.subscriptionName)
            //                     .subscriptionType(subscriptionDto.subscriptionType)
            //                     .keySharedPolicy(subscriptionDto.keySharedPolicy.policy)
            //                     .deadLetterPolicy(DeadLetterPolicy.builder()
            //                         .maxRedeliverCount(10)
            //                         .deadLetterTopic(subscriptionDto.deadLetterTopic)
            //                         .build()
            //                     )
            //         ));

            // ReactivePulsarContainerProperties<T> containerProps = new ReactivePulsarContainerProperties<>();
            // containerProps.setSubscriptionName(subscriptionDto.subscriptionName);
            // containerProps.setSubscriptionType(subscriptionDto.subscriptionType);
            // containerProps.setConcurrency(0);
            // containerProps.setSchema(Schema.JSON(getClazz()));
                // .isAckReceiptEnabled(true)
                // .messageListener(listener)
                // .subscribe();
     */

    @PreDestroy
    public void destroy() {
        this.listeners.values().forEach(MessageListenerContainer::destroy);
    }
}
