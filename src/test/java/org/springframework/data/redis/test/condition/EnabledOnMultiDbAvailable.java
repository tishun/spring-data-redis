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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;

/**
 * {@code @EnabledOnMultiDbAvailable} signals that the annotated test class or test method is only <em>enabled</em> if
 * the Mode M1 multi-database setup is reachable. Mode M1 reuses the shared standalone nodes
 * ({@link org.springframework.data.redis.SettingsUtils#getMultiDbPortA() 6379} and
 * {@link org.springframework.data.redis.SettingsUtils#getMultiDbPortB() 6382}) as independent stand-in regions; no
 * dedicated infrastructure is required.
 *
 * @author Tihomir Mateev
 */
@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
@ExtendWith(EnabledOnMultiDbAvailableCondition.class)
public @interface EnabledOnMultiDbAvailable {
}
