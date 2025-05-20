package io.awspring.cloud.sqs.listener.errorhandler;

import io.awspring.cloud.sqs.listener.SqsHeaders;
import io.awspring.cloud.sqs.listener.Visibility;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link ExponentialBackoffErrorHandlerWithFullJitter} and {@link ExponentialBackoffErrorHandlerWithHalfJitter}.
 *
 * @author Bruno Garcia
 * @author Rafael Pavarini
 */
class ExponentialBackoffErrorHandlerJitterTest extends BaseExponentialBackoffErrorHandlerJitterTest {
	static Supplier<Random> midRandomSupplier = () -> new MockedRandomNextInt(timeout -> timeout / 2);
	static Supplier<Random> maxRandomSupplier = () -> new MockedRandomNextInt(timeout -> timeout - 1);

	@ParameterizedTest
	@MethodSource("testCases")
	void calculateExponentialFullJitter(BaseTestCase baseTestCase) {
		Message<Object> message = mock(Message.class);
		RuntimeException exception = new RuntimeException("Expected exception from shouldChangeVisibilityToZero");
		MessageHeaders headers = mock(MessageHeaders.class);
		Visibility visibility = mock(Visibility.class);
		given(message.getHeaders()).willReturn(headers);
		given(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER, Visibility.class)).willReturn(visibility);
		given(headers.get(SqsHeaders.MessageSystemAttributes.SQS_APPROXIMATE_RECEIVE_COUNT, String.class))
			.willReturn(baseTestCase.sqsApproximateReceiveCount);
		given(visibility.changeToAsync(anyInt())).willReturn(CompletableFuture.completedFuture(null));


		assertThat(baseTestCase.calculateWithVisibilityTimeoutExpectedFullJitter(message, exception)).isCompletedExceptionally();
		then(visibility).should().changeToAsync(baseTestCase.VisibilityTimeoutExpectedFullJitter);
	}

	@ParameterizedTest
	@MethodSource("testCases")
	void calculateExponentialHalfJitter(BaseTestCase baseTestCase) {
		Message<Object> message = mock(Message.class);
		RuntimeException exception = new RuntimeException("Expected exception from shouldChangeVisibilityToZero");
		MessageHeaders headers = mock(MessageHeaders.class);
		Visibility visibility = mock(Visibility.class);
		given(message.getHeaders()).willReturn(headers);
		given(headers.get(SqsHeaders.SQS_VISIBILITY_TIMEOUT_HEADER, Visibility.class)).willReturn(visibility);
		given(headers.get(SqsHeaders.MessageSystemAttributes.SQS_APPROXIMATE_RECEIVE_COUNT, String.class))
			.willReturn(baseTestCase.sqsApproximateReceiveCount);
		given(visibility.changeToAsync(anyInt())).willReturn(CompletableFuture.completedFuture(null));

		assertThat(baseTestCase.calculateWithVisibilityTimeoutExpectedHalfJitter(message, exception)).isCompletedExceptionally();
 		then(visibility).should().changeToAsync(baseTestCase.VisibilityTimeoutExpectedHalfJitter);

	}

	private static Collection<BaseTestCase> testCases() {
		return List.of(
			baseTestCaseMidRandomSupplier()
				.sqsApproximateReceiveCount("1")
				.VisibilityTimeoutExpectedHalfJitter((int) ((BackoffVisibilityConstants.DEFAULT_INITIAL_VISIBILITY_TIMEOUT_SECONDS * 1.5) / 2))
				.VisibilityTimeoutExpectedFullJitter(BackoffVisibilityConstants.DEFAULT_INITIAL_VISIBILITY_TIMEOUT_SECONDS / 2),

			baseTestCaseMidRandomSupplier()
				.sqsApproximateReceiveCount("2")
				.VisibilityTimeoutExpectedHalfJitter(150)
				.VisibilityTimeoutExpectedFullJitter(100),


			baseTestCaseMidRandomSupplier()
				.sqsApproximateReceiveCount("3")
				.VisibilityTimeoutExpectedHalfJitter(300)
				.VisibilityTimeoutExpectedFullJitter(200),


			baseTestCaseMidRandomSupplier()
				.sqsApproximateReceiveCount("5")
				.VisibilityTimeoutExpectedHalfJitter(1200)
				.VisibilityTimeoutExpectedFullJitter(800),


			baseTestCaseMidRandomSupplier()
				.sqsApproximateReceiveCount("7")
				.VisibilityTimeoutExpectedHalfJitter(4800)
				.VisibilityTimeoutExpectedFullJitter(3200),


			baseTestCaseMidRandomSupplier()
				.sqsApproximateReceiveCount("11")
				.VisibilityTimeoutExpectedHalfJitter(Visibility.MAX_VISIBILITY_TIMEOUT_SECONDS / 2 + Visibility.MAX_VISIBILITY_TIMEOUT_SECONDS / 4)
				.VisibilityTimeoutExpectedFullJitter(Visibility.MAX_VISIBILITY_TIMEOUT_SECONDS / 2),


			baseTestCaseMidRandomSupplier()
				.sqsApproximateReceiveCount("13")
				.VisibilityTimeoutExpectedHalfJitter(Visibility.MAX_VISIBILITY_TIMEOUT_SECONDS / 2 + Visibility.MAX_VISIBILITY_TIMEOUT_SECONDS / 4)
				.VisibilityTimeoutExpectedFullJitter(Visibility.MAX_VISIBILITY_TIMEOUT_SECONDS / 2),

			baseTestCaseMaxRandomSupplier()
				.sqsApproximateReceiveCount("1")
				.VisibilityTimeoutExpectedHalfJitter(BackoffVisibilityConstants.DEFAULT_INITIAL_VISIBILITY_TIMEOUT_SECONDS)
				.VisibilityTimeoutExpectedFullJitter(BackoffVisibilityConstants.DEFAULT_INITIAL_VISIBILITY_TIMEOUT_SECONDS),

			baseTestCaseMaxRandomSupplier()
				.sqsApproximateReceiveCount("2")
				.VisibilityTimeoutExpectedHalfJitter(200)
				.VisibilityTimeoutExpectedFullJitter(200),

			baseTestCaseMaxRandomSupplier()
				.sqsApproximateReceiveCount("3")
				.VisibilityTimeoutExpectedHalfJitter(400)
				.VisibilityTimeoutExpectedFullJitter(400),

			baseTestCaseMaxRandomSupplier()
				.sqsApproximateReceiveCount("5")
				.VisibilityTimeoutExpectedHalfJitter(1600)
				.VisibilityTimeoutExpectedFullJitter(1600),

			baseTestCaseMaxRandomSupplier()
				.sqsApproximateReceiveCount("7")
				.VisibilityTimeoutExpectedHalfJitter(6400)
				.VisibilityTimeoutExpectedFullJitter(6400),

			baseTestCaseMaxRandomSupplier()
				.sqsApproximateReceiveCount("11")
				.VisibilityTimeoutExpectedHalfJitter(Visibility.MAX_VISIBILITY_TIMEOUT_SECONDS)
				.VisibilityTimeoutExpectedFullJitter(Visibility.MAX_VISIBILITY_TIMEOUT_SECONDS),

			baseTestCaseMaxRandomSupplier()
				.sqsApproximateReceiveCount("13")
				.VisibilityTimeoutExpectedHalfJitter(Visibility.MAX_VISIBILITY_TIMEOUT_SECONDS)
				.VisibilityTimeoutExpectedFullJitter(Visibility.MAX_VISIBILITY_TIMEOUT_SECONDS)
		);
	}

	private static BaseTestCase baseTestCaseMidRandomSupplier() {
		return new BaseTestCase()
			.initialVisibilityTimeoutSeconds(BackoffVisibilityConstants.DEFAULT_INITIAL_VISIBILITY_TIMEOUT_SECONDS)
			.randomSupplier(midRandomSupplier)
			.multiplier(BackoffVisibilityConstants.DEFAULT_MULTIPLIER);
	}

	private static BaseTestCase baseTestCaseMaxRandomSupplier() {
		return new BaseTestCase()
			.initialVisibilityTimeoutSeconds(BackoffVisibilityConstants.DEFAULT_INITIAL_VISIBILITY_TIMEOUT_SECONDS)
			.randomSupplier(maxRandomSupplier)
			.multiplier(BackoffVisibilityConstants.DEFAULT_MULTIPLIER);
	}
}
