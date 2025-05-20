package io.awspring.cloud.sqs.listener.errorhandler;

import io.awspring.cloud.sqs.listener.Visibility;
import org.springframework.messaging.Message;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class BaseExponentialBackoffErrorHandlerJitterTest {
	static class BaseTestCase {
		String sqsApproximateReceiveCount;
		Supplier<Random> randomSupplier;
		int initialVisibilityTimeoutSeconds;
		double multiplier;
		int VisibilityTimeoutExpectedFullJitter;
		int VisibilityTimeoutExpectedHalfJitter;

		BaseTestCase sqsApproximateReceiveCount(String sqsApproximateReceiveCount) {
			this.sqsApproximateReceiveCount = sqsApproximateReceiveCount;
			return this;
		}

		BaseTestCase randomSupplier(Supplier<Random> randomSupplier) {
			this.randomSupplier = randomSupplier;
			return this;
		}

		BaseTestCase initialVisibilityTimeoutSeconds(int initialVisibilityTimeoutSeconds) {
			this.initialVisibilityTimeoutSeconds = initialVisibilityTimeoutSeconds;
			return this;
		}

		BaseTestCase multiplier(double multiplier) {
			this.multiplier = multiplier;
			return this;
		}

		BaseTestCase VisibilityTimeoutExpectedHalfJitter(int VisibilityTimeoutExpectedHalfJitter) {
			this.VisibilityTimeoutExpectedHalfJitter = VisibilityTimeoutExpectedHalfJitter;
			return this;
		}

		BaseTestCase VisibilityTimeoutExpectedFullJitter(int VisibilityTimeoutExpectedFullJitter) {
			this.VisibilityTimeoutExpectedFullJitter = VisibilityTimeoutExpectedFullJitter;
			return this;
		}

		CompletableFuture<Void> calculateWithVisibilityTimeoutExpectedHalfJitter(Message<Object> message, Throwable t) {
			ExponentialBackoffErrorHandlerWithHalfJitter<Object> handler = ExponentialBackoffErrorHandlerWithHalfJitter
				.builder()
				.randomSupplier(this.randomSupplier)
				.initialVisibilityTimeoutSeconds(this.initialVisibilityTimeoutSeconds)
				.multiplier(this.multiplier)
				.build();

			return handler.handle(message, t);
		}

		CompletableFuture<Void> calculateWithVisibilityTimeoutExpectedHalfJitter(Collection<Message<Object>> messages, Throwable t) {
			ExponentialBackoffErrorHandlerWithHalfJitter<Object> handler = ExponentialBackoffErrorHandlerWithHalfJitter
				.builder()
				.randomSupplier(this.randomSupplier)
				.initialVisibilityTimeoutSeconds(this.initialVisibilityTimeoutSeconds)
				.multiplier(this.multiplier)
				.build();

			return handler.handle(messages, t);
		}

		CompletableFuture<Void> calculateWithVisibilityTimeoutExpectedFullJitter(Message<Object> message, Throwable t) {
			ExponentialBackoffErrorHandlerWithFullJitter<Object> handler = ExponentialBackoffErrorHandlerWithFullJitter
				.builder()
				.randomSupplier(this.randomSupplier)
				.initialVisibilityTimeoutSeconds(this.initialVisibilityTimeoutSeconds)
				.multiplier(this.multiplier)
				.build();

			return handler.handle(message, t);
		}

		CompletableFuture<Void> calculateWithVisibilityTimeoutExpectedFullJitter(Collection<Message<Object>> messages, Throwable t) {
			ExponentialBackoffErrorHandlerWithFullJitter<Object> handler = ExponentialBackoffErrorHandlerWithFullJitter
				.builder()
				.randomSupplier(this.randomSupplier)
				.initialVisibilityTimeoutSeconds(this.initialVisibilityTimeoutSeconds)
				.multiplier(this.multiplier)
				.build();

			return handler.handle(messages, t);
		}
	}

	 static class MockedRandomNextInt extends Random {
		final Function<Integer, Integer> nextInt;

		 MockedRandomNextInt(Function<Integer, Integer> nextInt) {
			this.nextInt = nextInt;
		}

		@Override
		public int nextInt(int bound) {
			return nextInt.apply(bound);
		}

		 @Override
		 public int nextInt(int origin, int bound) {
			 return nextInt.apply(bound);
		 }
	}
}
