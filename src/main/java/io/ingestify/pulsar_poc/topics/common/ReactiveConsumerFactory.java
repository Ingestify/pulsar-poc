package io.ingestify.pulsar_poc.topics.common;

import java.util.List;

import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.pulsar.reactive.config.DefaultReactivePulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.core.DefaultReactivePulsarConsumerFactory;
import org.springframework.pulsar.reactive.core.ReactivePulsarConsumerFactory;
import org.springframework.pulsar.reactive.listener.ReactivePulsarContainerProperties;
import org.springframework.stereotype.Component;


@Component
public class ReactiveConsumerFactory<T> {

    @Autowired
    private ReactivePulsarClient reactivePulsarClient;

    public ReactivePulsarListenerContainerFactory<T> createConsumerFactory(String topic, SubscriptionData subscriptionData, ReactivePulsarContainerProperties<T> containerProps) {
        ReactivePulsarConsumerFactory<T> consumerFactory =
                new DefaultReactivePulsarConsumerFactory<T>(
                    reactivePulsarClient,
                    List.of(
                        (c) -> c.topic(topic)
                                .consumerName(subscriptionData.subscriptionName() + "-consumer")
                                .subscriptionName(subscriptionData.subscriptionName())
                                .subscriptionType(subscriptionData.subscriptionType())
                                // .keySharedPolicy(subscriptionData.keySharedPolicy().policy)
                                .deadLetterPolicy(subscriptionData.deadLetterPolicy())
                    ));
                    
        return new DefaultReactivePulsarListenerContainerFactory<>(consumerFactory, containerProps);
    }

}
