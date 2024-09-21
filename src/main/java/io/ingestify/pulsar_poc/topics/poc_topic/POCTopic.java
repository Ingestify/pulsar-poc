package io.ingestify.pulsar_poc.topics.poc_topic;

import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.springframework.messaging.Message;
import org.springframework.pulsar.reactive.support.MessageUtils;
import org.springframework.stereotype.Component;

import io.ingestify.pulsar_poc.topics.common.StreamingPulsarTopic;
import lombok.Getter;
import reactor.core.publisher.Flux;

@Component
public class POCTopic extends StreamingPulsarTopic<String> {
    @Getter
    public final String topicName = "persistent://tenant/namespace/poc-topic";

    @Override
    public Flux<MessageResult<Void>> listen(Flux<Message<String>> messages) {
        return messages
            .doOnNext((msg) -> System.out.println("Received: " + msg.getPayload()))
            .map(MessageUtils::acknowledge);
    }

    @Override
    protected Class<String> getClazz() {
        return String.class;
    }

    @Override
    protected SchemaType getSchemaType() {
        return SchemaType.STRING;
    }
}