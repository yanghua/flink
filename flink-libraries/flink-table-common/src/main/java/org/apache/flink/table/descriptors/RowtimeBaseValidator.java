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

/**
 * Validator for [[Rowtime]].
 */
public abstract class RowtimeBaseValidator implements DescriptorValidator {

	public static final String ROWTIME = "rowtime";
	public static final String ROWTIME_TIMESTAMPS_TYPE = "rowtime.timestamps.type";
	public static final String ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD = "from-field";
	public static final String ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_SOURCE = "from-source";
	public static final String ROWTIME_TIMESTAMPS_TYPE_VALUE_CUSTOM = "custom";
	public static final String ROWTIME_TIMESTAMPS_FROM = "rowtime.timestamps.from";
	public static final String ROWTIME_TIMESTAMPS_CLASS = "rowtime.timestamps.class";
	public static final String ROWTIME_TIMESTAMPS_SERIALIZED = "rowtime.timestamps.serialized";

	public static final String ROWTIME_WATERMARKS_TYPE = "rowtime.watermarks.type";
	public static final String ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_ASCENDING = "periodic-ascending";
	public static final String ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_BOUNDED = "periodic-bounded";
	public static final String ROWTIME_WATERMARKS_TYPE_VALUE_FROM_SOURCE = "from-source";
	public static final String ROWTIME_WATERMARKS_TYPE_VALUE_CUSTOM = "custom";
	public static final String ROWTIME_WATERMARKS_CLASS = "rowtime.watermarks.class";
	public static final String ROWTIME_WATERMARKS_SERIALIZED = "rowtime.watermarks.serialized";
	public static final String ROWTIME_WATERMARKS_DELAY = "rowtime.watermarks.delay";

	protected boolean supportsSourceTimestamps;
	protected boolean supportsSourceWatermarks;
	protected String prefix;

	public RowtimeBaseValidator(boolean supportsSourceTimestamps, boolean supportsSourceWatermarks, String prefix) {
		this.supportsSourceTimestamps = supportsSourceTimestamps;
		this.supportsSourceWatermarks = supportsSourceWatermarks;
		this.prefix = prefix;
	}

}
