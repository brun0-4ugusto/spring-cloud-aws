/*
 * Copyright 2013-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.awspring.cloud.sqs.listener.errorhandler;

import io.awspring.cloud.sqs.MessageHeaderUtils;
import io.awspring.cloud.sqs.listener.QueueMessageVisibility;
import io.awspring.cloud.sqs.listener.SqsHeaders;
import io.awspring.cloud.sqs.listener.Visibility;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * An implementation of an Exponential Backoff error handler for asynchronous message processing.
 *
 * <p>
 * This error handler sets the SQS message visibility timeout exponentially
 * based on the number of received attempts whenever an exception occurs.
 *
 * <p>
 * When AcknowledgementMode is set to ON_SUCCESS (the default),
 * returning a failed future prevents the message from being acknowledged.
 *
 * @author Bruno Garcia
 * @author Rafael Pavarini
 */

public class ExponentialBackoffErrorHandler<T> implements AsyncErrorHandler<T> {
	/**
	 * The default initial visibility timeout interval.
	 */
	public static final int DEFAULT_INITIAL_VISIBILITY_TIMEOUT_SECONDS = 100;
	/**
	 * The default multiplier, which doubles the visibility timeout.
	 */
	public static final double DEFAULT_MULTIPLIER = 2.0;
	/**
	 * The default maximum visibility timeout interval,
	 * which corresponds to the maximum SQS visibility timeout of 12 hours.
	 */
	public static final int DEFAULT_MAX_VISIBILITY_TIMEOUT_SECONDS = 43200;

	private final int initialVisibilityTimeoutSeconds;
	private final double multiplier;
	private final int maxVisibilityTimeoutSeconds;

	public ExponentialBackoffErrorHandler() {
		this(DEFAULT_INITIAL_VISIBILITY_TIMEOUT_SECONDS, DEFAULT_MULTIPLIER, DEFAULT_MAX_VISIBILITY_TIMEOUT_SECONDS);
	}

	public ExponentialBackoffErrorHandler(int initialVisibilityTimeoutSeconds, double multiplier) {
		this(initialVisibilityTimeoutSeconds, multiplier, DEFAULT_MAX_VISIBILITY_TIMEOUT_SECONDS);
	}

	public ExponentialBackoffErrorHandler(int initialVisibilityTimeoutSeconds, double multiplier, int maxVisibilityTimeoutSeconds) {
		checkVisibilityTimeout(initialVisibilityTimeoutSeconds);
		checkMultiplier(multiplier);
		checkVisibilityTimeout(maxVisibilityTimeoutSeconds);
		Assert.isTrue(initialVisibilityTimeoutSeconds <= maxVisibilityTimeoutSeconds,
			"Initial visibility timeout must not exceed max visibility timeout");
		this.initialVisibilityTimeoutSeconds = initialVisibilityTimeoutSeconds;
		this.multiplier = multiplier;
		this.maxVisibilityTimeoutSeconds = maxVisibilityTimeoutSeconds;
	}

	@Override
	public CompletableFuture<Void> handle(Message<T> message, Throwable t) {
		return applyExponentialBackoffVisibilityTimeout(message)
			.thenCompose(theVoid -> CompletableFuture.failedFuture(t));
	}

	@Override
	public CompletableFuture<Void> handle(Collection<Message<T>> messages, Throwable t) {
		return applyExponentialBackoffVisibilityTimeout(messages)
			.thenCompose(theVoid -> CompletableFuture.failedFuture(t));
	}

	private CompletableFuture<Void> applyExponentialBackoffVisibilityTimeout(Collection<Message<T>> messages) {
		CompletableFuture<?>[] futures = messages.stream()
			.collect(Collectors.groupingBy(this::getReceiveMessageCount))
			.entrySet()
			.stream()
			.map(entry -> {
				int timeout = calculateVisibilityTimeout(entry.getKey());

				QueueMessageVisibility firstVisibilityMessage = (QueueMessageVisibility) getVisibilityTimeout(entry.getValue().get(0));

				Collection<Message<?>> batch = new ArrayList<>(entry.getValue());

				return firstVisibilityMessage.toBatchVisibility(batch).changeToAsync(timeout);
			}).toArray(CompletableFuture[]::new);

		return CompletableFuture.allOf(futures);
	}

	private CompletableFuture<Void> applyExponentialBackoffVisibilityTimeout(Message<T> message) {
		Visibility visibilityTimeout = getVisibilityTimeout(message);
		long receiveMessageCount = getReceiveMessageCount(message);
		int timeout = calculateVisibilityTimeout(receiveMessageCount);
		return visibilityTimeout.changeToAsync(timeout);
	}

	private int calculateVisibilityTimeout(long receiveMessageCount) {
		double exponential = initialVisibilityTimeoutSeconds * Math.pow(multiplier, receiveMessageCount - 1);
		int seconds = (int) Math.min(exponential, (long) Integer.MAX_VALUE);
		return Math.min(seconds, maxVisibilityTimeoutSeconds);
	}

	private long getReceiveMessageCount(Message<T> message) {
		return Long.parseLong(MessageHeaderUtils.getHeader(message, SqsHeaders.MessageSystemAttributes.SQS_APPROXIMATE_RECEIVE_COUNT, String.class));

	}

	private Visibility getVisibilityTimeout(Message<T> message) {
		return MessageHeaderUtils.getHeader(message, SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER, Visibility.class);
	}

	private void checkVisibilityTimeout(long visibilityTimeout) {
		Assert.isTrue(visibilityTimeout > 0, () -> "Invalid visibility timeout '" +
			visibilityTimeout + "'. Should be greater than 0 ");
		Assert.isTrue(visibilityTimeout <= DEFAULT_MAX_VISIBILITY_TIMEOUT_SECONDS, () -> "Invalid visibility timeout '" +
			visibilityTimeout + "'. Should be less than or equal to " + DEFAULT_MAX_VISIBILITY_TIMEOUT_SECONDS);
	}

	private void checkMultiplier(double multiplier) {
		Assert.isTrue(multiplier >= 1, () -> "Invalid multiplier '" + multiplier + "'. Should be greater than " +
			"or equal to 1.");
	}
}
