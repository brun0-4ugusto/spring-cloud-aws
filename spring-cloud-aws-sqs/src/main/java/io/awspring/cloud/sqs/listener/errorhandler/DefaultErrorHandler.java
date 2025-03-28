package io.awspring.cloud.sqs.listener.errorhandler;

import io.awspring.cloud.sqs.CompletableFutures;
import io.awspring.cloud.sqs.listener.SqsHeaders;
import io.awspring.cloud.sqs.listener.Visibility;
import org.springframework.messaging.Message;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class DefaultErrorHandler<T> implements AsyncErrorHandler<T> {


	public CompletableFuture<Void> handle(Message<T> message, Throwable t) {
		Visibility visibility = getVisibilityTimeout(message);

		return CompletableFutures.exceptionallyCompose(
			changeTimeoutToZero(visibility),
			CompletableFutures::failedFuture
		);
	}


	private Visibility getVisibilityTimeout(Message<T> message) {
		Object visibility = message.getHeaders().get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER);
		if (Objects.isNull(visibility) || !(visibility instanceof Visibility)) {
			throw new RuntimeException("Invalid visibility visibility header");
		}
		return (Visibility) visibility;
	}


	private CompletableFuture<Void> changeTimeoutToZero(Visibility visibility) {
		return visibility.changeToAsync(0);
	}


}
