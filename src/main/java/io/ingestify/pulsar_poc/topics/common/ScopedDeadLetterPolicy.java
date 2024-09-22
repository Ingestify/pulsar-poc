package io.ingestify.pulsar_poc.topics.common;

import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component("scopedDeadLetterPolicy")
@Scope("request")
@Lazy
public class ScopedDeadLetterPolicy extends DeadLetterPolicy {
    public ScopedDeadLetterPolicy(int maxRedeliverCount, String retryLetterTopic, String deadLetterTopic, String initialSubscriptionName) {
        super(maxRedeliverCount, retryLetterTopic, deadLetterTopic, initialSubscriptionName);
    }
}
