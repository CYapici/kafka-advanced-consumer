package com.cagatayyapici.kafka.consumer.util;

import com.google.common.base.Strings;

/**
 * 
 * @author cagatayyapici
 *
 */
public class Validator {

	public static void validateArgs(String groupID, String topics, long pollTimeOut) {
		if (Strings.isNullOrEmpty(groupID)) {
			throw new IllegalArgumentException("Parameter Group Id cannot be null or empty");
		}
		if (Strings.isNullOrEmpty(topics)) {
			throw new IllegalArgumentException("Parameter Topic List  cannot be null or empty");
		}
		if (pollTimeOut < 0) {
			throw new IllegalArgumentException("Parameter Poll Timeout can not be less than zero.");
		}

	}

}
