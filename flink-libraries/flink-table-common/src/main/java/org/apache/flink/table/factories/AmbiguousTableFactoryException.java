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

package org.apache.flink.table.factories;

import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Exception for finding more than one [[TableFactory]] for the given properties.
 */
public class AmbiguousTableFactoryException extends RuntimeException {

	public AmbiguousTableFactoryException(
		Collection<TableFactory> matchingFactories,
		Class<?> factoryClass,
		Collection<TableFactory> factories,
		Map<String, String> properties,
		Throwable cause) {

		super("More than one suitable table factory for '"
				+ factoryClass.getName() + "' could be found in the classpath. The following factories match: "
				+ matchingFactories.stream().map(item -> item.getClass().getName()).collect(Collectors.joining("\n"))
				+ "The following properties are requested:"
				+ DescriptorProperties.toString(properties) + " The following factories have been considered: "
				+ factories.stream().map(item -> item.getClass().getName()).collect(Collectors.joining("\n")),
			cause);
	}

	public AmbiguousTableFactoryException(
		Collection<TableFactory> matchingFactories,
		Class<?> factoryClass,
		Collection<TableFactory> factories,
		Map<String, String> properties) {

		this(matchingFactories, factoryClass, factories, properties, null);
	}

}
