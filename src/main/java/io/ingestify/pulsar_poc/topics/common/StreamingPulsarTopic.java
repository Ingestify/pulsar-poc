package io.ingestify.pulsar_poc.topics.common;

import java.lang.reflect.Method;

import org.apache.pulsar.reactive.client.api.MessageResult;
import lombok.SneakyThrows;
import reactor.core.publisher.Flux;

// import org.apache.pulsar.client.api.Message;
import org.springframework.messaging.Message;

public non-sealed abstract class StreamingPulsarTopic<T> extends ReactivePulsarTopicConsumner<T> {
    
    public abstract Flux<MessageResult<Void>>listen(Flux<Message<T>> message);

    @SneakyThrows
    @Override
    protected Method getMethod() {
        var method = this.getClass().getMethods()[0];
        
        return method;
    }
}
