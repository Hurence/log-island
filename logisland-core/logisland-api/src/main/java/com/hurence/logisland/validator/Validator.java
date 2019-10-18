/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.validator;

import java.io.Serializable;

/**
 *
 */
public interface Validator extends Serializable {

    /**
     * Validator object providing validation behavior in which validation always
     * fails
     */
    Validator INVALID = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input, ValidationContext context) {
            return new ValidationResult.Builder().subject(subject).explanation(String.format("'%s' is not a supported property", subject)).input(input).build();
        }
        @Override
        public ValidationResult validate(final String subject, final String input) {
            return new ValidationResult.Builder().subject(subject).explanation(String.format("'%s' is not a supported property", subject)).input(input).build();
        }
    };

    /**
     * Validator object providing validation behavior in which validation always
     * passes
     */
    Validator VALID = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input, ValidationContext context) {
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        }
        @Override
        public ValidationResult validate(final String subject, final String input) {
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        }
    };

    /**
     * @param subject what is being validated
     * @param input the string to be validated
     * @return ValidationResult
     * @throws NullPointerException of given input is null
     */
    ValidationResult validate(String subject, String input, ValidationContext context);
    ValidationResult validate(String subject, String input);
}
