package io.awspring.cloud.sqs.listener.errorhandler;

import io.awspring.cloud.sqs.CompletableFutures;
import io.awspring.cloud.sqs.listener.SqsHeaders;
import io.awspring.cloud.sqs.listener.Visibility;
import io.awspring.cloud.sqs.listener.sink.FanOutMessageSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * A default error handler implementation for asynchronous message processing.
 *
 * <p>This error handler attempts to set the SQS message visibility timeout to zero whenever an
 * exception occurs, effectively making the message immediately available for reprocessing. If
 * no visibility timeout header is found, the handler will throw a {@link RuntimeException}.
 *
 * @author Bruno Augusto Garcia
 * @author Rafael Condez Pavarini
 * @since 3.3
 */
public class DefaultErrorHandler<T> implements AsyncErrorHandler<T> {
	Logger logger = LoggerFactory.getLogger(DefaultErrorHandler.class);


	@Override
	public CompletableFuture<Void> handle(Message<T> message, Throwable t) {
		return CompletableFutures.exceptionallyCompose(
			changeTimeoutToZero(message),
			CompletableFutures::failedFuture
		);
	}

	@Override
	public CompletableFuture<Void> handle(Collection<Message<T>> messages, Throwable t) {
		return changeTimeoutToZeroCollectionOfMessages(messages);
	}


	private CompletableFuture<Void> changeTimeoutToZeroCollectionOfMessages(Collection<Message<T>> messages) {
		return CompletableFuture.allOf(
			messages.stream()
				.map(msg -> changeTimeoutToZero(msg)
					.exceptionally(throwable -> logError(throwable, msg)))
				.toArray(CompletableFuture[]::new)
		);
	}

	private Void logError(Throwable t, Message<?> message) {
		logger.error("Message Not Recovery {}. Exception: {}", t.getMessage(), t);
		return null;
	}


	private CompletableFuture<Void> changeTimeoutToZero(Message<T> message) {
		Visibility visibilityTimeout = getVisibilityTimeout(message);
		return visibilityTimeout.changeToAsync(0);
	}


	private Visibility getVisibilityTimeout(Message<T> message) {
		Object visibility = message.getHeaders().get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER);
		if (Objects.isNull(visibility) || !(visibility instanceof Visibility)) {
			throw new RuntimeException("Invalid visibility header");
		}
		return (Visibility) visibility;
	}
}
