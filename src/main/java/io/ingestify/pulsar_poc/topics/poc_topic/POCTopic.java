package io.ingestify.pulsar_poc.topics.poc_topic;

import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.springframework.context.annotation.Lazy;
import org.springframework.messaging.Message;
import org.springframework.pulsar.reactive.support.MessageUtils;
import org.springframework.stereotype.Component;

import io.ingestify.pulsar_poc.topics.common.StreamingPulsarTopic;
import reactor.core.publisher.Flux;

@Component("pocTopic")
@Lazy
public class POCTopic extends StreamingPulsarTopic<String> {
    
    @Override
    public Flux<MessageResult<Void>> listen(Flux<Message<String>> messages) {
        return messages
            .doOnNext((msg) -> System.out.println("Received: " + msg.getPayload()))
            .map(MessageUtils::acknowledge);
    }

    @Override
    protected SchemaType getSchemaType() {
        return SchemaType.STRING;
    }

    @Override
    public String getBeanName() {
        return "pocTopic";
    }
    @Override
    public Object getSelf() {
        return this;
    }
}