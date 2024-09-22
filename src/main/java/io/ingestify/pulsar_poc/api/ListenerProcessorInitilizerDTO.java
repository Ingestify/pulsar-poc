package io.ingestify.pulsar_poc.api;

import java.util.Optional;

import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.pulsar.config.PulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListenerMessageConsumerBuilderCustomizer;

import io.ingestify.pulsar_poc.topics.common.ListenerProcessorInitilizer;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
@Setter
public class ListenerProcessorInitilizerDTO {
    
	/**
	 * Pulsar subscription name associated with this listener.
	 * <p>
	 * SpEL {@code #{...}} and property placeholders {@code ${...}} are supported.
	 * @return the subscription name for this listener
	 */
	String subscriptionName;

	/**
	 * Topics to listen to.
	 * <p>
	 * SpEL {@code #{...}} and property placeholders {@code ${...}} are supported.
	 * @return an array of topics to listen to
	 */
	String[] topics = {};

	/**
	 * Pulsar subscription type for this listener - expected to be a single element array
	 * with subscription type or empty array to indicate null type.
	 * @return single element array with the subscription type or empty array to indicate
	 * no type chosen by user
	 */
	Optional<SubscriptionType> subscriptionType = Optional.empty();

	/**
	 * The bean name of the {@link PulsarListenerContainerFactory} to use to create the
	 * message listener container responsible to serve this endpoint.
	 * <p>
	 * If not specified, the default container factory is used, if any. If a SpEL
	 * expression is provided ({@code #{...}}), the expression can either evaluate to a
	 * container factory instance or a bean name.
	 * @return the container factory bean name.
	 */
	Optional<String> containerFactory = Optional.empty();

	/**
	 * Topic patten to listen to.
	 * <p>
	 * SpEL {@code #{...}} and property placeholders {@code ${...}} are supported.
	 * @return topic pattern to listen to
	 */
	Optional<String> topicPattern = Optional.empty();

	/**
	 * Whether to automatically start the container for this listener.
	 * <p>
	 * The value can be a literal string representation of boolean (e.g. {@code 'true'})
	 * or a property placeholder {@code ${...}} that resolves to a literal. SpEL
	 * {@code #{...}} expressions that evaluate to a {@link Boolean} or a literal are
	 * supported.
	 * @return whether to automatically start the container for this listener
	 */
	Optional<Boolean> autoStartup = Optional.of(true);

	/**
	 * Activate stream consumption.
	 * @return if true, the listener method shall take a
	 * {@link reactor.core.publisher.Flux} as input argument.
	 */
	Optional<Boolean> stream = Optional.empty();

	/**
	 * Override the container factory's {@code concurrency} setting for this listener.
	 * <p>
	 * The value can be a literal string representation of {@link Number} (e.g.
	 * {@code '3'}) or a property placeholder {@code ${...}} that resolves to a literal.
	 * SpEL {@code #{...}} expressions that evaluate to a {@link Number} or a literal are
	 * supported.
	 * @return the concurrency for this listener
	 */
	Optional<String> concurrency = Optional.empty();

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
	Optional<Boolean> useKeyOrderedProcessing = Optional.empty();

	/**
	 * The bean name or a SpEL expression that resolves to a
	 * {@link ReactivePulsarListenerMessageConsumerBuilderCustomizer} to use to configure
	 * the underlying consumer.
	 * @return the bean name or SpEL expression to the customizer or an empty string to
	 * not customize the consumer
	 */
	Optional<String> consumerCustomizer = Optional.empty();

    Optional<DeadLetterPolicy> deadLetterPolicy = Optional.empty();

	public ListenerProcessorInitilizer buildInitilizer() {
		var builder = ListenerProcessorInitilizer.builder();

		builder.subscriptionName(subscriptionName);
		builder.topics(topics);
		
		subscriptionType.ifPresent(p -> builder.subscriptionType(new SubscriptionType[]{p}));
		containerFactory.ifPresent(p -> builder.containerFactory(p));
		topicPattern.ifPresent(p -> builder.topicPattern(p));
		autoStartup.ifPresent(p -> builder.autoStartup(Boolean.toString(p)));
		stream.ifPresent(p -> builder.stream(p));
		concurrency.ifPresent(p -> builder.concurrency(p));
		useKeyOrderedProcessing.ifPresent(p -> builder.useKeyOrderedProcessing(Boolean.toString(p)));
		consumerCustomizer.ifPresent(p -> builder.consumerCustomizer(p));
		builder.dlp(deadLetterPolicy);

		return builder.build();
	}
}