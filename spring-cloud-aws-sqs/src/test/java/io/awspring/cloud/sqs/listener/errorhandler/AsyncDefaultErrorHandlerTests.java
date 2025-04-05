package io.awspring.cloud.sqs.listener.errorhandler;

import io.awspring.cloud.sqs.listener.SqsHeaders;
import io.awspring.cloud.sqs.listener.Visibility;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link AsyncDefaultErrorHandler}.
 *
 * @author Bruno Augusto Garcia
 * @author Rafael Condez Pavarini
 */
@SuppressWarnings("unchecked")
class AsyncDefaultErrorHandlerTests {

	@Test
	void shouldChangeVisibilityToZero() {
		Message<Object> message = mock(Message.class);
		RuntimeException exception = new RuntimeException("Expected exception from shouldChangeVisibilityToZero");
		MessageHeaders headers = mock(MessageHeaders.class);
		Visibility visibility = mock(Visibility.class);
		when(message.getHeaders()).thenReturn(headers);
		when(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER, Visibility.class)).thenReturn(visibility);
		when(visibility.changeToAsync(0)).thenReturn(CompletableFuture.completedFuture(null));


		AsyncDefaultErrorHandler<Object> handler = new AsyncDefaultErrorHandler<>();
		CompletableFuture<Void> future = handler.handle(message, exception);

		assertThat(future).isCompletedWithValue(null);
		verify(visibility).changeToAsync(0);
	}

	@Test
	void shouldReturnError() {
		Message<Object> message = mock(Message.class);
		RuntimeException exception = new RuntimeException("Expected exception from shouldReturnError");
		MessageHeaders headers = mock(MessageHeaders.class);
		when(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER, Visibility.class)).thenReturn(null);
		when(message.getHeaders()).thenReturn(headers);

		AsyncDefaultErrorHandler<Object> handler = new AsyncDefaultErrorHandler<>();

		assertThat(handler.handle(message, exception)).isCompletedExceptionally();
	}

	@Test
	void shouldChangeVisibilityToZeroBatch() {
		Message<Object> message1 = mock(Message.class);
		Message<Object> message2 = mock(Message.class);
		Message<Object> message3 = mock(Message.class);
		List<Message<Object>> batch = Arrays.asList(message1, message2, message3);
		RuntimeException exception = new RuntimeException("Expected exception from shouldChangeVisibilityToZeroBatch");
		MessageHeaders headers = mock(MessageHeaders.class);
		Visibility visibility = mock(Visibility.class);
		when(visibility.changeToAsync(0)).thenReturn(CompletableFuture.completedFuture(null));
		when(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER, Visibility.class)).thenReturn(visibility);
		when(message1.getHeaders()).thenReturn(headers);
		when(message2.getHeaders()).thenReturn(headers);
		when(message3.getHeaders()).thenReturn(headers);

		AsyncDefaultErrorHandler<Object> handler = new AsyncDefaultErrorHandler<>();
		CompletableFuture<Void> future = handler.handle(batch, exception);

		assertThat(future).isCompletedWithValue(null);
		verify(visibility, times(3)).changeToAsync(0);

		when(visibility.changeToAsync(0)).thenReturn(CompletableFuture.failedFuture(exception));

		future = handler.handle(batch, exception);

		assertThat(future).isCompletedWithValue(null);
		verify(visibility, times(6)).changeToAsync(0);
	}


	@Test
	void shouldNotReturnCompletableFailedAtBatch() {
		Message<Object> message1 = mock(Message.class);
		Message<Object> message2 = mock(Message.class);
		Message<Object> message3 = mock(Message.class);
		List<Message<Object>> batch = Arrays.asList(message1, message2, message3);
		RuntimeException exception = new RuntimeException("Expected exception from shouldReturnErrorBatch");
		MessageHeaders headers = mock(MessageHeaders.class);
		when(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER, Visibility.class)).thenReturn(null);
		when(message1.getHeaders()).thenReturn(headers);
		when(message2.getHeaders()).thenReturn(headers);
		when(message3.getHeaders()).thenReturn(headers);
		AsyncDefaultErrorHandler<Object> handler = new AsyncDefaultErrorHandler<>();

		assertThat(handler.handle(batch, exception)).isCompletedWithValue(null);

		Visibility visibility = mock(Visibility.class);
		when(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER, Visibility.class))
			.thenReturn(visibility);
		when(visibility.changeToAsync(0))
			.thenReturn(
				CompletableFuture.completedFuture(null),
				CompletableFuture.failedFuture(new RuntimeException("Visibility change failed")),
				CompletableFuture.completedFuture(null)
			);
		assertThat(handler.handle(batch, exception)).isCompletedWithValue(null);
		verify(visibility, times(3)).changeToAsync(0);
	}
}
