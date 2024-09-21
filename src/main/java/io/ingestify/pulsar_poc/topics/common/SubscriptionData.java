package io.ingestify.pulsar_poc.topics.common;

import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.SubscriptionType;

public record SubscriptionData(
    // String topic, // example: "persistent://tenant/namespace/test"
    String subscriptionName,
    SubscriptionType subscriptionType,
    // String deadLetterTopic, // example: "persistent://tenant/namespace/dead-letter-topic"
    KeySharedPolicyEnum keySharedPolicy, // example: KeySharedPolicy.autoSplitHashRange()
    // Integer maxRedeliverCount,
    DeadLetterPolicy deadLetterPolicy
) {
    public enum KeySharedPolicyEnum {
        AUTO_SPLIT(KeySharedPolicy.autoSplitHashRange()),
        STICKY(KeySharedPolicy.stickyHashRange());

        private KeySharedPolicyEnum(KeySharedPolicy policy) {
            this.policy = policy;
        }
        
        public KeySharedPolicy policy;
    }
}