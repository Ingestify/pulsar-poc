package io.ingestify.pulsar_poc.topics.common;

import java.util.Optional;

import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;
import org.springframework.pulsar.config.PulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerEndpointRegistry;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListenerMessageConsumerBuilderCustomizer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ListenerProcessorInitilizer {
    
    /**
	 * The unique identifier of the container for this listener.
	 * <p>
	 * If none is specified an auto-generated id is used.
	 * <p>
	 * SpEL {@code #{...}} and property placeholders {@code ${...}} are supported.
	 * @return the {@code id} for the container managing for this endpoint.
	 * @see ReactivePulsarListenerEndpointRegistry#getListenerContainer(String)
	 */
    @Builder.Default
	String id = "";

	/**
	 * Pulsar subscription name associated with this listener.
	 * <p>
	 * SpEL {@code #{...}} and property placeholders {@code ${...}} are supported.
	 * @return the subscription name for this listener
	 */
    @Builder.Default
	String subscriptionName = "";

	/**
	 * Pulsar subscription type for this listener - expected to be a single element array
	 * with subscription type or empty array to indicate null type.
	 * @return single element array with the subscription type or empty array to indicate
	 * no type chosen by user
	 */
    @Builder.Default
	SubscriptionType[] subscriptionType = { SubscriptionType.Exclusive };

	/**
	 * Pulsar schema type for this listener.
	 * @return the {@code schemaType} for this listener
	 */
    @Builder.Default
	SchemaType schemaType = SchemaType.NONE;

	/**
	 * The bean name of the {@link PulsarListenerContainerFactory} to use to create the
	 * message listener container responsible to serve this endpoint.
	 * <p>
	 * If not specified, the default container factory is used, if any. If a SpEL
	 * expression is provided ({@code #{...}}), the expression can either evaluate to a
	 * container factory instance or a bean name.
	 * @return the container factory bean name.
	 */
    @Builder.Default
	String containerFactory = "";

	/**
	 * Topics to listen to.
	 * <p>
	 * SpEL {@code #{...}} and property placeholders {@code ${...}} are supported.
	 * @return an array of topics to listen to
	 */
    @Builder.Default
	String[] topics = {};

	/**
	 * Topic patten to listen to.
	 * <p>
	 * SpEL {@code #{...}} and property placeholders {@code ${...}} are supported.
	 * @return topic pattern to listen to
	 */
    @Builder.Default
	String topicPattern = "";

	/**
	 * Whether to automatically start the container for this listener.
	 * <p>
	 * The value can be a literal string representation of boolean (e.g. {@code 'true'})
	 * or a property placeholder {@code ${...}} that resolves to a literal. SpEL
	 * {@code #{...}} expressions that evaluate to a {@link Boolean} or a literal are
	 * supported.
	 * @return whether to automatically start the container for this listener
	 */
    @Builder.Default
	String autoStartup = "";

	/**
	 * Activate stream consumption.
	 * @return if true, the listener method shall take a
	 * {@link reactor.core.publisher.Flux} as input argument.
	 */
    @Builder.Default
	boolean stream = false;

	/**
	 * A pseudo bean name used in SpEL expressions within this annotation to reference the
	 * current bean within which this listener is defined. This allows access to
	 * properties and methods within the enclosing bean. Default '__listener'.
	 * <p>
	 * @return the pseudo bean name.
	 */
    @Builder.Default
	String beanRef = "__listener";

	/**
	 * Override the container factory's {@code concurrency} setting for this listener.
	 * <p>
	 * The value can be a literal string representation of {@link Number} (e.g.
	 * {@code '3'}) or a property placeholder {@code ${...}} that resolves to a literal.
	 * SpEL {@code #{...}} expressions that evaluate to a {@link Number} or a literal are
	 * supported.
	 * @return the concurrency for this listener
	 */
    @Builder.Default
	String concurrency = "";

	/**
	 * Set to true or false, to override the default setting in the container factory. May
	 * be a property placeholder or SpEL expression that evaluates to a {@link Boolean} or
	 * a {@link String}, in which case the {@link Boolean#parseBoolean(String)} is used to
	 * obtain the value.
	 * <p>
	 * SpEL {@code #{...}} and property place holders {@code ${...}} are supported.
	 * @return true to keep ordering by message key when concurrency > 1, false to not
	 * keep ordering.
	 */
    @Builder.Default
	String useKeyOrderedProcessing = "";

	/**
	 * The bean name or a SpEL expression that resolves to a
	 * {@link org.apache.pulsar.client.api.DeadLetterPolicy} to use on the consumer to
	 * configure a dead letter policy for message redelivery.
	 * @return the bean name or empty string to not set any dead letter policy.
	 */
	public String getDeadLetterPolicy() {
		return dlp.map(d -> "scopedDeadLetterPolicy").orElse("");
	}

	/**
	 * The bean name or a SpEL expression that resolves to a
	 * {@link ReactivePulsarListenerMessageConsumerBuilderCustomizer} to use to configure
	 * the underlying consumer.
	 * @return the bean name or SpEL expression to the customizer or an empty string to
	 * not customize the consumer
	 */
    @Builder.Default
	String consumerCustomizer = "";

	@Builder.Default
    Optional<DeadLetterPolicy> dlp = Optional.empty();


    // public enum KeySharedPolicyEnum {
    //     AUTO_SPLIT(KeySharedPolicy.autoSplitHashRange()),
    //     STICKY(KeySharedPolicy.stickyHashRange());

    //     private KeySharedPolicyEnum(KeySharedPolicy policy) {
    //         this.policy = policy;
    //     }
        
    //     public KeySharedPolicy policy;
    // }
}