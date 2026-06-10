/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.redis.test.condition;

import static org.junit.jupiter.api.extension.ConditionEvaluationResult.*;

import java.util.Optional;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.util.AnnotationUtils;

import org.springframework.data.redis.SettingsUtils;

/**
 * {@link ExecutionCondition} for {@link EnabledOnMultiDbAvailable @EnabledOnMultiDbAvailable}. Skips when either of the
 * two Mode M1 ports is unreachable, mirroring the existing
 * {@link EnabledOnRedisSentinelAvailable @EnabledOnRedisSentinelAvailable} semantics.
 *
 * @author Tihomir Mateev
 */
class EnabledOnMultiDbAvailableCondition implements ExecutionCondition {

	private static final ConditionEvaluationResult ENABLED_BY_DEFAULT = enabled(
			"@EnabledOnMultiDbAvailable is not present");

	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {

		Optional<EnabledOnMultiDbAvailable> optional = AnnotationUtils.findAnnotation(context.getElement(),
				EnabledOnMultiDbAvailable.class);

		if (optional.isEmpty()) {
			return ENABLED_BY_DEFAULT;
		}

		int portA = SettingsUtils.getMultiDbPortA();
		int portB = SettingsUtils.getMultiDbPortB();

		if (RedisDetector.canConnectToPort(portA) && RedisDetector.canConnectToPort(portB)) {
			return enabled("Connection successful to multi-database endpoints at %s:%d and %s:%d"
					.formatted(SettingsUtils.getHost(), portA, SettingsUtils.getHost(), portB));
		}

		return disabled("Cannot connect to multi-database endpoints at %s:%d and %s:%d".formatted(SettingsUtils.getHost(),
				portA, SettingsUtils.getHost(), portB));
	}
}
