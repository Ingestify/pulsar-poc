package io.ingestify.pulsar_poc.topics.containers;


// import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import io.ingestify.pulsar_poc.topics.common.StreamingPulsarTopic;
import lombok.Getter;
import reactor.core.publisher.Flux;

@Component
public class ContainersTopic extends StreamingPulsarTopic<ContainersAction> {

    @Getter
    public final String topicName = "containers-topic";

    @Override
    public Flux<MessageResult<Void>> listen(Flux<Message<ContainersAction>> message) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'listenStream'");
    }

    @Override
    protected Class<ContainersAction> getClazz() {
        return ContainersAction.class;
    }
    
    @Override
    protected SchemaType getSchemaType() {
        return SchemaType.INT16;
    }
}
