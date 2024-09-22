package io.ingestify.pulsar_poc.topics.common;

import java.lang.reflect.Method;

import org.apache.pulsar.client.api.Message;

import lombok.SneakyThrows;
import reactor.core.publisher.Mono;

public non-sealed abstract class OneByOnePulsarTopic<T> extends ReactivePulsarTopicConsumner<T> {
    public abstract Mono<Void> listen(Mono<Message<T>> message);

    @SneakyThrows
    @Override
    protected Method getMethod() {
        var method = this.getClass().getMethods()[0];
        
        return method;
    }
}
