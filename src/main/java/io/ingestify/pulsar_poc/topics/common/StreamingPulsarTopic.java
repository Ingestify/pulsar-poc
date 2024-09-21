package io.ingestify.pulsar_poc.topics.common;

import java.lang.reflect.Method;

// import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.springframework.messaging.Message;

import lombok.SneakyThrows;
import reactor.core.publisher.Flux;

public non-sealed abstract class StreamingPulsarTopic<T> extends ReactivePulsarTopic<T> {
    public abstract Flux<MessageResult<Void>>listen(Flux<Message<T>> message);

    @SneakyThrows
    @Override
    protected Method getMethod() {
        return this.getClass().getMethods()[0];
        // return this.getClass().getMethod("listen", Flux.class);
    }
}
