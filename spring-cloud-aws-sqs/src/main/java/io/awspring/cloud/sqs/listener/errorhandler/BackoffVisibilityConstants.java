package io.awspring.cloud.sqs.listener.errorhandler;

import io.awspring.cloud.sqs.listener.Visibility;
/**
 * Constants for Backoff Error Handler.
 *
 * @author Bruno Garcia
 * @author Rafael Pavarini
 */
public class BackoffVisibilityConstants {
	/**
	 * The default initial visibility timeout.
	 */
	 static final int DEFAULT_INITIAL_VISIBILITY_TIMEOUT_SECONDS = 100;

	/**
	 * The default multiplier, which doubles the visibility timeout.
	 */
	 static final double DEFAULT_MULTIPLIER = 2.0;
	 static final int DEFAULT_MAX_VISIBILITY_TIMEOUT_SECONDS = Visibility.MAX_VISIBILITY_TIMEOUT_SECONDS;
}
