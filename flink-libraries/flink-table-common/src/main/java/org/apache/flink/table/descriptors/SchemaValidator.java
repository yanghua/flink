/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.descriptors;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.RowtimeBaseValidator.ROWTIME;
import static org.apache.flink.table.descriptors.RowtimeBaseValidator.ROWTIME_TIMESTAMPS_CLASS;
import static org.apache.flink.table.descriptors.RowtimeBaseValidator.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.RowtimeBaseValidator.ROWTIME_TIMESTAMPS_SERIALIZED;
import static org.apache.flink.table.descriptors.RowtimeBaseValidator.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.RowtimeBaseValidator.ROWTIME_WATERMARKS_CLASS;
import static org.apache.flink.table.descriptors.RowtimeBaseValidator.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.RowtimeBaseValidator.ROWTIME_WATERMARKS_SERIALIZED;
import static org.apache.flink.table.descriptors.RowtimeBaseValidator.ROWTIME_WATERMARKS_TYPE;

/**
 * Validator for [[Schema]].
 */
public class SchemaValidator implements DescriptorValidator {

	/**
	 * Prefix for schema-related properties.
	 */
	public static final String SCHEMA = "schema";
	public static final String SCHEMA_NAME = "name";
	public static final String SCHEMA_TYPE = "type";
	public static final String SCHEMA_PROCTIME = "proctime";
	public static final String SCHEMA_FROM = "from";

	private boolean isStreamEnvironment;
	private boolean supportsSourceTimestamps;
	private boolean supportsSourceWatermarks;

	public SchemaValidator(
		boolean isStreamEnvironment,
		boolean supportsSourceTimestamps,
		boolean supportsSourceWatermarks) {

		this.isStreamEnvironment = isStreamEnvironment;
		this.supportsSourceTimestamps = supportsSourceTimestamps;
		this.supportsSourceWatermarks = supportsSourceWatermarks;
	}

	public void validate(DescriptorProperties properties) {
		Map<String, String> names = properties.getIndexedProperty(SCHEMA, SCHEMA_NAME);
		Map<String, String> types = properties.getIndexedProperty(SCHEMA, SCHEMA_TYPE);

		if (names.isEmpty() && types.isEmpty()) {
			throw new ValidationException(
				"Could not find the required schema in property '" + SCHEMA + "'.");
		}

		boolean proctimeFound = false;

		for (int i = 0; i <= Math.max(names.size(), types.size()); i++) {
			properties
				.validateString(SCHEMA + "." + i + "." + SCHEMA_NAME, false, 1);
			properties
				.validateType(SCHEMA + "." + i + "." + SCHEMA_TYPE, false, false);
			properties
				.validateString(SCHEMA + "." + i + "." + SCHEMA_FROM, true, 1);
			// either proctime or rowtime
			String proctime = SCHEMA + "." + i + "." + SCHEMA_PROCTIME;
			String rowtime = SCHEMA + "." + i + "." + ROWTIME;
			if (properties.containsKey(proctime)) {
				// check the environment
				if (!isStreamEnvironment) {
					throw new ValidationException(
						"Property '" + proctime + "' is not allowed in a batch environment.");
				}
				// check for only one proctime attribute
				else if (proctimeFound) {
					throw new ValidationException("A proctime attribute must only be defined once.");
				}
				// check proctime
				properties.validateBoolean(proctime, false);
				proctimeFound = properties.getBoolean(proctime);
				// no rowtime
				properties.validatePrefixExclusion(rowtime);
			} else if (properties.hasPrefix(rowtime)) {
				// check rowtime
				RowtimeBaseValidator rowtimeValidator = new RowtimeBaseValidator(
					supportsSourceTimestamps,
					supportsSourceWatermarks,
					SCHEMA + "." + i + ".");
				rowtimeValidator.validate(properties);
				// no proctime
				properties.validateExclusion(proctime);
			}
		}
	}


	/**
	 * Returns keys for a
	 * [[org.apache.flink.table.factories.TableFormatFactory.supportedProperties()]] method that
	 * are accepted for schema derivation using [[deriveFormatFields(DescriptorProperties)]].
	 */
	public List<String> getSchemaDerivationKeys() {
		List<String> keys = new ArrayList<String>();

		// schema
		keys.add(SCHEMA + ".#." + SCHEMA_TYPE);
		keys.add(SCHEMA + ".#." + SCHEMA_NAME);
		keys.add(SCHEMA + ".#." + SCHEMA_FROM);

		// time attributes
		keys.add(SCHEMA + ".#." + SCHEMA_PROCTIME);
		keys.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE);
		keys.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM);
		keys.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS);
		keys.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED);
		keys.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE);
		keys.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS);
		keys.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED);
		keys.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY);

		return keys;
	}

	// utilities

	/**
	 * Finds the proctime attribute if defined.
	 */
	public static Optional<String> deriveProctimeAttribute(DescriptorProperties properties) {
		Map<String, String> names = properties.getIndexedProperty(SCHEMA, SCHEMA_NAME);

		for (int i = 0; i < names.size(); i++) {
			Optional<Boolean> isProctime =
				properties.getOptionalBoolean(SCHEMA + "." + i + "." + SCHEMA_PROCTIME);
			if (isProctime.isPresent()) {
				if (isProctime.get()) {
					return Optional.of(names.get(SCHEMA + "." + i + "." + SCHEMA_NAME));
				}
			}
		}

		return Optional.empty();
	}

	/**
	 * Finds the rowtime attributes if defined.
	 */
	public static List<RowtimeAttributeDescriptor> deriveRowtimeAttributes(DescriptorProperties properties) {

		Map<String, String> names = properties.getIndexedProperty(SCHEMA, SCHEMA_NAME);

		List<RowtimeAttributeDescriptor> attributes = new ArrayList<>();

		// check for rowtime in every field
		for (int i = 0; i < names.size(); i++) {
			RowtimeBaseValidator
				.getRowtimeComponents(properties, SCHEMA + "." + i + ".")
				.foreach { case (extractor, strategy) =>
				// create descriptor
				attributes += new RowtimeAttributeDescriptor(
					properties.getString(SCHEMA + "." + i + "." + SCHEMA_NAME),
					extractor,
					strategy);
			}
		}

		return attributes;
	}

	/**
	 * Finds a table source field mapping.
	 *
	 * @param properties The properties describing a schema.
	 * @param inputType  The input type that a connector and/or format produces. This parameter
	 *                   can be used to resolve a rowtime field against an input field.
	 */
	public static Map<String, String> deriveFieldMapping(
		DescriptorProperties properties, Optional<TypeInformation<?>> inputType) {

		Map<String, String> mapping = new HashMap<>();

		TableSchema schema = properties.getTableSchema(SCHEMA);

		String[] columnNames;
		if (inputType.isPresent() && inputType.get() instanceof CompositeType) {
			columnNames = ((CompositeType) inputType.get()).getFieldNames();
		} else {
			columnNames = new String[0];
		}

		// add all source fields first because rowtime might reference one of them
		for (String columnName : columnNames) {
			mapping.put(columnName, columnName);
		}

		// add all schema fields first for implicit mappings
		for (String fieldName : schema.getFieldNames()) {
			mapping.put(fieldName, fieldName);
		}

		Map<String, String> names = properties.getIndexedProperty(SCHEMA, SCHEMA_NAME);

		for (int i = 0; i < names.size(); i++) {
			String name = properties.getString(SCHEMA + "." + i + "." + SCHEMA_NAME);
			Optional<String> value = properties.getOptionalString(SCHEMA + "." + i + "." + SCHEMA_FROM);

			// add explicit mapping
			if (value.isPresent()) {
				String source = value.get();
				mapping.put(name, source);
			}
			// implicit mapping or time
			else {
				boolean isProctime = properties
					.getOptionalBoolean(SCHEMA + "." + i + "." + SCHEMA_PROCTIME)
					.orElse(false);
				boolean isRowtime = properties
					.containsKey(SCHEMA + "." + i + "." + ROWTIME_TIMESTAMPS_TYPE);
				List<String> columnNameList = Arrays.asList(columnNames);
				// remove proctime/rowtime from mapping
				if (isProctime || isRowtime) {
					mapping.remove(name);
				}
				// check for invalid fields
				else if (!columnNameList.contains(name)) {
					throw new ValidationException("Could not map the schema field '" + name + "' to a field " +
						"from source. Please specify the source field from which it can be derived.");
				}
			}
		}

		return mapping;
	}

}
