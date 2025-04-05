package io.awspring.cloud.sqs.listener.errorhandler;

import io.awspring.cloud.sqs.MessageHeaderUtils;
import io.awspring.cloud.sqs.listener.BatchVisibility;
import io.awspring.cloud.sqs.listener.SqsHeaders;
import io.awspring.cloud.sqs.listener.Visibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * A default error handler implementation for asynchronous message processing.
 *
 * <p>This error handler attempts to set the SQS message visibility timeout to zero whenever an
 * exception occurs, effectively making the message immediately available for reprocessing.
 *
 * @author Bruno Augusto Garcia
 * @author Rafael Pavarini
 */
public class AsyncDefaultErrorHandler<T> implements AsyncErrorHandler<T> {

	private final Logger logger = LoggerFactory.getLogger(AsyncDefaultErrorHandler.class);

	@Override
	public CompletableFuture<Void> handle(Message<T> message, Throwable t) {
		return changeTimeoutToZero(message);
	}

	@Override
	public CompletableFuture<Void> handle(Collection<Message<T>> messages, Throwable t) {
		return changeTimeoutToZero(messages);
	}

	private CompletableFuture<Void> changeTimeoutToZero(Message<T> message) {
		try {
			Visibility visibilityTimeout = getVisibilityTimeout(message);
			return visibilityTimeout.changeToAsync(0);
		} catch (Exception e) {
			return CompletableFuture.failedFuture(e);
		}
	}

	private CompletableFuture<Void> changeTimeoutToZero(Collection<Message<T>> messages) {
		return CompletableFuture.allOf(
			messages.stream()
				.map(msg -> changeTimeoutToZero(msg)
					.exceptionally(this::doSomethingWhenMessageIsNotRecoverableInBatch))
				.toArray(CompletableFuture[]::new)
		);
	}

	private Visibility getVisibilityTimeout(Message<T> message) {
		return MessageHeaderUtils.getHeader(message, SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER, Visibility.class);
	}

	/**
	 * This method can be overridden, for now the default implementation is just log.
	 */
	protected Void doSomethingWhenMessageIsNotRecoverableInBatch(Throwable t) {
		logger.error("Message Not Recovery {}. Exception: ", t.getMessage(), t);
		return null;
	}

}
