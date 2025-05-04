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

import io.awspring.cloud.sqs.listener.BatchVisibility;
import io.awspring.cloud.sqs.listener.QueueMessageVisibility;
import io.awspring.cloud.sqs.listener.SqsHeaders;
import io.awspring.cloud.sqs.listener.Visibility;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/**
 * Tests for {@link ExponentialBackoffErrorHandler}.
 *
 * @author Bruno Garcia
 * @author Rafael Pavarini
 */
class ExponentialBackoffErrorHandlerTest {

	@Test
	void shouldChangeVisibilityTimeoutExponentiallyWithDefaultIntialVisibilityTimeout() {
		Message<Object> message = mock(Message.class);
		RuntimeException exception = new RuntimeException("Expected exception from shouldChangeVisibilityToZero");
		MessageHeaders headers = mock(MessageHeaders.class);
		Visibility visibility = mock(Visibility.class);
		given(message.getHeaders()).willReturn(headers);
		given(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER, Visibility.class)).willReturn(visibility);
		given(headers.get(SqsHeaders.MessageSystemAttributes.SQS_APPROXIMATE_RECEIVE_COUNT, String.class)).willReturn("1");
		given(visibility.changeToAsync(anyInt())).willReturn(CompletableFuture.completedFuture(null));

		ExponentialBackoffErrorHandler<Object> handler = new ExponentialBackoffErrorHandler<>();

		assertThat(handler.handle(message, exception)).isCompletedExceptionally();
		then(visibility).should().changeToAsync(ExponentialBackoffErrorHandler.DEFAULT_INITIAL_VISIBILITY_TIMEOUT_SECONDS);
	}

	@Test
	void shouldChangeVisibilityTimeoutExponentiallyWithDefaultMaxVisibilityTimeout() {
		Message<Object> message = mock(Message.class);
		RuntimeException exception = new RuntimeException("Expected exception from shouldChangeVisibilityToZero");
		MessageHeaders headers = mock(MessageHeaders.class);
		Visibility visibility = mock(Visibility.class);
		given(message.getHeaders()).willReturn(headers);
		given(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER, Visibility.class)).willReturn(visibility);
		given(headers.get(SqsHeaders.MessageSystemAttributes.SQS_APPROXIMATE_RECEIVE_COUNT, String.class)).willReturn("1");
		given(visibility.changeToAsync(anyInt())).willReturn(CompletableFuture.completedFuture(null));

		ExponentialBackoffErrorHandler<Object> handler = new ExponentialBackoffErrorHandler<>(ExponentialBackoffErrorHandler.DEFAULT_MAX_VISIBILITY_TIMEOUT_SECONDS, 2);

		assertThat(handler.handle(message, exception)).isCompletedExceptionally();
		then(visibility).should().changeToAsync(ExponentialBackoffErrorHandler.DEFAULT_MAX_VISIBILITY_TIMEOUT_SECONDS);
	}

	@Test
	void shouldChangeVisibilityTimeoutExponentiallyWithCustomVisibilityTimeout() {
		Message<Object> message = mock(Message.class);
		RuntimeException exception = new RuntimeException("Expected exception from shouldChangeVisibilityToZero");
		MessageHeaders headers = mock(MessageHeaders.class);
		Visibility visibility = mock(Visibility.class);
		given(message.getHeaders()).willReturn(headers);
		given(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER, Visibility.class)).willReturn(visibility);
		given(headers.get(SqsHeaders.MessageSystemAttributes.SQS_APPROXIMATE_RECEIVE_COUNT, String.class)).willReturn("1");
		given(visibility.changeToAsync(anyInt())).willReturn(CompletableFuture.completedFuture(null));

		ExponentialBackoffErrorHandler<Object> handler = new ExponentialBackoffErrorHandler<>(500, 2);

		assertThat(handler.handle(message, exception)).isCompletedExceptionally();
		then(visibility).should().changeToAsync(500);
	}

	@Test
	void shouldChangeVisibilityTimeoutExponentiallyWithCustomVisibilityTimeoutAndMaxVisibilityTimeout() {
		Message<Object> message = mock(Message.class);
		RuntimeException exception = new RuntimeException("Expected exception from shouldChangeVisibilityToZero");
		MessageHeaders headers = mock(MessageHeaders.class);
		Visibility visibility = mock(Visibility.class);
		given(message.getHeaders()).willReturn(headers);
		given(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER, Visibility.class)).willReturn(visibility);
		given(headers.get(SqsHeaders.MessageSystemAttributes.SQS_APPROXIMATE_RECEIVE_COUNT, String.class)).willReturn("1");
		given(visibility.changeToAsync(anyInt())).willReturn(CompletableFuture.completedFuture(null));

		ExponentialBackoffErrorHandler<Object> handler = new ExponentialBackoffErrorHandler<>(500, 2, 501);

		assertThat(handler.handle(message, exception)).isCompletedExceptionally();
		then(visibility).should().changeToAsync(500);

		given(headers.get(SqsHeaders.MessageSystemAttributes.SQS_APPROXIMATE_RECEIVE_COUNT, String.class)).willReturn("2");
		assertThat(handler.handle(message, exception)).isCompletedExceptionally();
		then(visibility).should().changeToAsync(501);

	}

	@Test
	void shouldChangeVisibilityTimeoutExponentially() {
		Message<Object> message = mock(Message.class);
		RuntimeException exception = new RuntimeException("Expected exception from shouldChangeVisibilityToZero");
		MessageHeaders headers = mock(MessageHeaders.class);
		Visibility visibility = mock(Visibility.class);
		given(message.getHeaders()).willReturn(headers);
		given(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER, Visibility.class)).willReturn(visibility);
		given(headers.get(SqsHeaders.MessageSystemAttributes.SQS_APPROXIMATE_RECEIVE_COUNT, String.class)).willReturn("1");
		given(visibility.changeToAsync(anyInt())).willReturn(CompletableFuture.completedFuture(null));

		ExponentialBackoffErrorHandler<Object> handler = new ExponentialBackoffErrorHandler<>(500, 2);

		assertThat(handler.handle(message, exception)).isCompletedExceptionally();
		then(visibility).should().changeToAsync(500);

		given(headers.get(SqsHeaders.MessageSystemAttributes.SQS_APPROXIMATE_RECEIVE_COUNT, String.class)).willReturn("2");
		assertThat(handler.handle(message, exception)).isCompletedExceptionally();
		then(visibility).should().changeToAsync(1000);

	}

	@Test
	void shouldChangeVisibilityTimeoutExponentiallyBatch() {
		Message<Object> message1 = mock(Message.class);
		Message<Object> message2 = mock(Message.class);
		Message<Object> message3 = mock(Message.class);
		List<Message<Object>> batch = Arrays.asList(message1, message2, message3);
		RuntimeException exception = new RuntimeException("Expected exception from shouldChangeVisibilityToZeroBatch");
		MessageHeaders headers = mock(MessageHeaders.class);
		MessageHeaders headers2 = mock(MessageHeaders.class);
		QueueMessageVisibility visibility = mock(QueueMessageVisibility.class);
		BatchVisibility batchvisibility = mock(BatchVisibility.class);
		given(batchvisibility.changeToAsync(anyInt())).willReturn(CompletableFuture.completedFuture(null));
		given(visibility.toBatchVisibility(any())).willReturn(batchvisibility);
		given(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER, Visibility.class)).willReturn(visibility);
		given(headers2.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER, Visibility.class)).willReturn(visibility);
		given(message1.getHeaders()).willReturn(headers);
		given(message2.getHeaders()).willReturn(headers2);
		given(message3.getHeaders()).willReturn(headers);

		given(headers.get(SqsHeaders.MessageSystemAttributes.SQS_APPROXIMATE_RECEIVE_COUNT, String.class)).willReturn("1");
		given(headers2.get(SqsHeaders.MessageSystemAttributes.SQS_APPROXIMATE_RECEIVE_COUNT, String.class)).willReturn("2");
		given(visibility.changeToAsync(anyInt())).willReturn(CompletableFuture.completedFuture(null));

		ExponentialBackoffErrorHandler<Object> handler = new ExponentialBackoffErrorHandler<>(500, 2);

		assertThat(handler.handle(batch, exception)).isCompletedExceptionally();
		then(batchvisibility).should(times(1)).changeToAsync(500);

		given(headers.get(SqsHeaders.MessageSystemAttributes.SQS_APPROXIMATE_RECEIVE_COUNT, String.class)).willReturn("2");
		given(headers2.get(SqsHeaders.MessageSystemAttributes.SQS_APPROXIMATE_RECEIVE_COUNT, String.class)).willReturn("3");
		assertThat(handler.handle(batch, exception)).isCompletedExceptionally();

		then(batchvisibility).should(times(2)).changeToAsync(1000);
		then(batchvisibility).should(times(1)).changeToAsync(2000);
	}

	@Test
	void shouldApplyMaxVisibilityTimeoutWhenCalculatedTimeoutExceedsLimit() {
		Message<Object> message = mock(Message.class);
		RuntimeException exception = new RuntimeException("Expected exception from shouldChangeVisibilityToZero");
		MessageHeaders headers = mock(MessageHeaders.class);
		Visibility visibility = mock(Visibility.class);
		given(message.getHeaders()).willReturn(headers);
		given(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER, Visibility.class)).willReturn(visibility);
		given(headers.get(SqsHeaders.MessageSystemAttributes.SQS_APPROXIMATE_RECEIVE_COUNT, String.class)).willReturn("11");
		given(visibility.changeToAsync(anyInt())).willReturn(CompletableFuture.completedFuture(null));

		ExponentialBackoffErrorHandler<Object> handler = new ExponentialBackoffErrorHandler<>(ExponentialBackoffErrorHandler.DEFAULT_MAX_VISIBILITY_TIMEOUT_SECONDS, Integer.MAX_VALUE);

		assertThat(handler.handle(message, exception)).isCompletedExceptionally();
		then(visibility).should().changeToAsync(ExponentialBackoffErrorHandler.DEFAULT_MAX_VISIBILITY_TIMEOUT_SECONDS);
	}
}
