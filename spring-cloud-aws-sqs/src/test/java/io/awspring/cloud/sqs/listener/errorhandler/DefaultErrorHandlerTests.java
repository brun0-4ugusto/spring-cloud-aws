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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link DefaultErrorHandler}.
 *
 * @author Tomaz Fernandes
 */
@SuppressWarnings("unchecked")
class DefaultErrorHandlerTests {

	@Test
	void shouldChangeVisibilityToZero(){
		Message<Object> message = mock(Message.class);
		RuntimeException exception = new RuntimeException("Expected exception from shouldChangeVisibilityToZero");
		MessageHeaders headers = mock(MessageHeaders.class);
		Visibility visibility = mock(Visibility.class);
		when(visibility.changeToAsync(0)).thenReturn(CompletableFuture.completedFuture(null));
		when(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER)).thenReturn(visibility);
		when(message.getHeaders()).thenReturn(headers);

		DefaultErrorHandler<Object> handler = new DefaultErrorHandler<>();
		CompletableFuture<Void> future = handler.handle(message, exception);

		assertThat(future).isCompletedWithValue(null);
		verify(visibility).changeToAsync(0);
	}

	@Test
	void shouldReturnError(){
		Message<Object> message = mock(Message.class);
		RuntimeException exception = new RuntimeException("Expected exception from shouldReturnError");
		MessageHeaders headers = mock(MessageHeaders.class);
		when(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER)).thenReturn(null);
		when(message.getHeaders()).thenReturn(headers);

		DefaultErrorHandler<Object> handler = new DefaultErrorHandler<>();

		assertThatThrownBy(() -> handler.handle(message, exception)).isInstanceOf(RuntimeException.class);

		when(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER)).thenReturn(new Object());

		assertThatThrownBy(() -> handler.handle(message, exception)).isInstanceOf(RuntimeException.class);

		when(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER)).thenReturn(new Object());
		when(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER)).thenReturn(new Object());
	}

	@Test
	void shouldChangeVisibilityToZeroBatch(){
		Message<Object> message1 = mock(Message.class);
		Message<Object> message2 = mock(Message.class);
		Message<Object> message3 = mock(Message.class);
		List<Message<Object>> batch = Arrays.asList(message1, message2, message3);
		RuntimeException exception = new RuntimeException("Expected exception from shouldChangeVisibilityToZeroBatch");
		MessageHeaders headers = mock(MessageHeaders.class);
		Visibility visibility = mock(Visibility.class);
		when(visibility.changeToAsync(0)).thenReturn(CompletableFuture.completedFuture(null));
		when(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER)).thenReturn(visibility);
		when(message1.getHeaders()).thenReturn(headers);
		when(message2.getHeaders()).thenReturn(headers);
		when(message3.getHeaders()).thenReturn(headers);

		DefaultErrorHandler<Object> handler = new DefaultErrorHandler<>();
		CompletableFuture<Void> future = handler.handle(batch, exception);

		assertThat(future).isCompletedWithValue(null);
		verify(visibility, times(3)).changeToAsync(0);

		when(visibility.changeToAsync(0)).thenReturn(CompletableFuture.failedFuture(exception));

		future = handler.handle(batch, exception);

		assertThat(future).isCompletedWithValue(null);
		verify(visibility, times(6)).changeToAsync(0);
	}


	@Test
	void shouldReturnErrorBatch(){
		Message<Object> message1 = mock(Message.class);
		Message<Object> message2 = mock(Message.class);
		Message<Object> message3 = mock(Message.class);
		List<Message<Object>> batch = Arrays.asList(message1, message2, message3);
		RuntimeException exception = new RuntimeException("Expected exception from shouldReturnErrorBatch");
		MessageHeaders headers = mock(MessageHeaders.class);
		when(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER)).thenReturn(null);
		when(message1.getHeaders()).thenReturn(headers);
		when(message2.getHeaders()).thenReturn(headers);
		when(message3.getHeaders()).thenReturn(headers);
		DefaultErrorHandler<Object> handler = new DefaultErrorHandler<>();

		assertThatThrownBy(() -> handler.handle(batch, exception))
			.isInstanceOf(RuntimeException.class);

		when(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER))
			.thenReturn(new Object());

		assertThatThrownBy(() -> handler.handle(batch, exception))
			.isInstanceOf(RuntimeException.class);
	}
}
