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

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.Option;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.MetadataValidator.METADATA_PROPERTY_VERSION;

/**
 * Unified interface to search for a [[TableFactory]] of provided type and properties.
 */
public class TableFactoryService<T> {

	private static final Logger LOG = LoggerFactory.getLogger(TableFactoryService.class);

	private static ServiceLoader<TableFactory> defaultLoader = ServiceLoader.load(TableFactory.class);

	public static final String STATISTICS_PROPERTY_VERSION = "statistics.property-version";

	/**
	 * Finds a table factory of the given class and descriptor.
	 *
	 * @param factoryClass desired factory class
	 * @param descriptor descriptor describing the factory configuration
	 * @tparam T factory class type
	 * @return the matching factory
	 */
	public static <T> T find(Class<T> factoryClass, Descriptor descriptor) {
		Preconditions.checkNotNull(descriptor);

		return findInternal(factoryClass, descriptor.toProperties(), null);
	}

	/**
	 * Finds a table factory of the given class, descriptor, and classloader.
	 *
	 * @param factoryClass desired factory class
	 * @param descriptor descriptor describing the factory configuration
	 * @param classLoader classloader for service loading
	 * @tparam T factory class type
	 * @return the matching factory
	 */
	public static <T> T find(Class<T> factoryClass, Descriptor descriptor, ClassLoader classLoader) {
		Preconditions.checkNotNull(descriptor);
		Preconditions.checkNotNull(classLoader);

		return findInternal(factoryClass, descriptor.toProperties(), classLoader);
	}

	/**
	 * Finds a table factory of the given class and property map.
	 *
	 * @param factoryClass desired factory class
	 * @param propertyMap properties that describe the factory configuration
	 * @tparam T factory class type
	 * @return the matching factory
	 */
	public static <T> T find(Class<T> factoryClass, Map<String, String> propertyMap) {
		return findInternal(factoryClass, propertyMap, null);
	}

	/**
	 * Finds a table factory of the given class, property map, and classloader.
	 *
	 * @param factoryClass desired factory class
	 * @param propertyMap properties that describe the factory configuration
	 * @param classLoader classloader for service loading
	 * @tparam T factory class type
	 * @return the matching factory
	 */
	public static <T> T find(Class<T> factoryClass, Map<String, String> propertyMap, ClassLoader classLoader) {
		Preconditions.checkNotNull(classLoader);

		return findInternal(factoryClass, propertyMap, classLoader);
	}

	/**
	 * Finds a table factory of the given class, property map, and classloader.
	 *
	 * @param factoryClass desired factory class
	 * @param propertyMap properties that describe the factory configuration
	 * @param classLoader classloader for service loading
	 * @tparam T factory class type
	 * @return the matching factory
	 */
	private static <T> T findInternal(
	Class<T> factoryClass,
	Map<String, String> propertyMap,
	ClassLoader classLoader) {

		Preconditions.checkNotNull(factoryClass);
		Preconditions.checkNotNull(propertyMap);

		Collection<TableFactory> foundFactories = discoverFactories(classLoader);

		Collection<TableFactory> classFactories = filterByFactoryClass(
			factoryClass,
			propertyMap,
			foundFactories);

		Collection<TableFactory> contextFactories = filterByContext(
			factoryClass,
			propertyMap,
			foundFactories,
			classFactories);

		return filterBySupportedProperties(
			factoryClass,
			propertyMap,
			foundFactories,
			contextFactories);
	}

	/**
	 * Searches for factories using Java service providers.
	 *
	 * @return all factories in the classpath
	 */
	private static Collection<TableFactory> discoverFactories(ClassLoader classLoader) {
		Iterator<TableFactory> iterator = null;
		try {

			if (classLoader != null) {
				ServiceLoader<TableFactory> loader = ServiceLoader.load(TableFactory.class, classLoader);
				iterator = loader.iterator();
			} else {
				iterator = defaultLoader.iterator();
			}
		} catch (Throwable e) {
			if (e instanceof ServiceConfigurationError) {
				LOG.error("Could not load service provider for table factories.", e);
				throw new TableException("Could not load service provider for table factories.", e);
			}
		}

		return IteratorUtils.toList(iterator);
	}

	/**
	 * Filters factories with matching context by factory class.
	 */
	private static <T> Collection<TableFactory> filterByFactoryClass(
	Class<T> factoryClass,
	Map<String, String> properties,
	Collection<TableFactory> foundFactories) {

		Collection<TableFactory> classFactories = foundFactories
			.stream()
			.filter(f -> factoryClass.isAssignableFrom(f.getClass()))
			.collect(Collectors.toList());

		if (classFactories.isEmpty()) {
			throw new NoMatchingTableFactoryException(
				"No factory implements '" + factoryClass.getCanonicalName() + "'.",
				factoryClass,
				foundFactories,
				properties);
		}

		return classFactories;
	}

	/**
	 * Filters for factories with matching context.
	 *
	 * @return all matching factories
	 */
	private static <T> Collection<TableFactory> filterByContext(
	Class<T> factoryClass,
	Map<String, String> properties,
	Collection<TableFactory> foundFactories,
	Collection<TableFactory> classFactories) {

		Collection<TableFactory> matchingFactories = classFactories.stream().filter(factory -> {
			Map<String, String> requestedContext = normalizeContext(factory);

			Map<String, String> plainContext = new HashMap<>();
			plainContext.putAll(requestedContext);
			// we remove the version for now until we have the first backwards compatibility case
			// with the version we can provide mappings in case the format changes
			plainContext.remove(CONNECTOR_PROPERTY_VERSION);
			plainContext.remove(FORMAT_PROPERTY_VERSION);
			plainContext.remove(METADATA_PROPERTY_VERSION);
			plainContext.remove(STATISTICS_PROPERTY_VERSION);

			// check if required context is met
			return plainContext.entrySet()
				.stream()
				.allMatch(e -> properties.containsKey(e.getKey()) && properties.get(e.getKey()) == e.getValue());
		}).collect(Collectors.toList());

		if (matchingFactories.isEmpty()) {
			throw new NoMatchingTableFactoryException(
				"No context matches.",
				factoryClass,
				foundFactories,
				properties);
		}

		return matchingFactories;
	}

	/**
	 * Prepares the properties of a context to be used for match operations.
	 */
	private static Map<String, String> normalizeContext(TableFactory factory) {
		Map<String, String> requiredContextJava = factory.requiredContext();
		if (requiredContextJava == null) {
			throw new TableException(
				"Required context of factory '" + factory.getClass().getName() + "' must not be null.");
		}
		return requiredContextJava.entrySet()
			.stream()
			.collect(Collectors.toMap(entry -> entry.getKey().toLowerCase(), entry -> entry.getValue()));
	}

	/**
	 * Filters the matching class factories by supported properties.
	 */
	private static <T> T filterBySupportedProperties(
		Class<T> factoryClass,
	 	Map<String, String> properties,
		Collection<TableFactory> foundFactories,
		Collection<TableFactory> classFactories) {

		List<String> plainGivenKeys = new ArrayList<>();
		properties.keySet().forEach(k -> {
			// replace arrays with wildcard
			String key = k.replaceAll(".\\d+", ".#");
			// ignore duplicates
			if (!plainGivenKeys.contains(key)) {
				plainGivenKeys.add(key);
			}
		});

		Optional<String> lastKey = null;
		Collection<TableFactory> supportedFactories = new ArrayList<>();

		for (TableFactory factory : classFactories) {
			Set<String> requiredContextKeys = normalizeContext(factory).keySet();
			Tuple2<Collection<String>, Collection<String>> entry = normalizeSupportedProperties(factory);
			Collection<String> supportedKeys = entry.f0;
			Collection<String> wildcards = entry.f1;

			// ignore context keys
			Collection<String> givenContextFreeKeys = plainGivenKeys
				.stream().filter(k -> !requiredContextKeys.contains(k)).collect(Collectors.toList());
			// perform factory specific filtering of keys
			Collection<String> givenFilteredKeys = filterSupportedPropertiesFactorySpecific(
				factory,
				givenContextFreeKeys);

			boolean allMatch = true;
			for (String key : givenFilteredKeys) {
				lastKey = Optional.of(key);
				allMatch = allMatch && (supportedKeys.contains(key) || wildcards.stream().anyMatch(key::startsWith));

				if (!allMatch) {
					break;
				}
			}

			if (allMatch) {
				supportedFactories.add(factory);
			}
		}

		if (supportedFactories.isEmpty() && classFactories.size() == 1 && lastKey != null) {
			// special case: when there is only one matching factory but the last property key
			// was incorrect
			TableFactory factory = classFactories.iterator().next();
			Tuple2<Collection<String>, Collection<String>> entry = normalizeSupportedProperties(factory);
			Collection<String> supportedKeys = entry.f0;
			throw new NoMatchingTableFactoryException(
				"The matching factory '" + factory.getClass().getName() + "' doesn't support '"
					+ lastKey.get() + "'." + "Supported properties of this factory are:"
          			+ supportedKeys.stream().sorted().collect(Collectors.joining("\n")),
			factoryClass,
				foundFactories,
				properties);
		} else if (supportedFactories.isEmpty()) {
			throw new NoMatchingTableFactoryException(
				"No factory supports all properties.",
				factoryClass,
				foundFactories,
				properties);
		} else if (supportedFactories.size() > 1) {
			throw new AmbiguousTableFactoryException(
				supportedFactories,
				factoryClass,
				foundFactories,
				properties);
		}

		return (T)supportedFactories.iterator().next();
	}

	/**
	 * Prepares the supported properties of a factory to be used for match operations.
	 */
	private static Tuple2<Collection<String>, Collection<String>> normalizeSupportedProperties(TableFactory factory) {
		List<String> supportedPropertiesJava = factory.supportedProperties();
		if (supportedPropertiesJava == null) {
			throw new TableException(
				"Supported properties of factory '" + factory.getClass().getName() + "' must not be null.");
		}
		List<String> supportedKeys = supportedPropertiesJava
			.stream()
			.map(s -> s.toLowerCase())
			.collect(Collectors.toList());

		// extract wildcard prefixes
		Collection<String> wildcards = extractWildcardPrefixes(supportedKeys);

		return new Tuple2<>(supportedKeys, wildcards);
	}

	/**
	 * Converts the prefix of properties with wildcards (e.g., "format.*").
	 */
	private static Collection<String> extractWildcardPrefixes(Collection<String> propertyKeys) {
		return propertyKeys
			.stream()
			.filter(s -> s.endsWith("*"))
			.map(s -> s.substring(0, s.length() - 1))
			.collect(Collectors.toList());
	}

	/**
	 * Performs filtering for special cases (i.e. table format factories with schema derivation).
	 */
	private static Collection<String> filterSupportedPropertiesFactorySpecific(
		TableFactory factory,
		Collection<String> keys) {

		if (factory instanceof TableFormatFactory) {
			TableFormatFactory formatFactory = (TableFormatFactory) factory;
			boolean includeSchema = formatFactory.supportsSchemaDerivation();
			return keys.stream().filter(k -> {
				if (includeSchema) {
					return k.startsWith(SchemaValidator.SCHEMA + ".") ||
						k.startsWith(FormatDescriptorValidator.FORMAT + ".");
				} else {
					return k.startsWith(FormatDescriptorValidator.FORMAT + ".");
				}
			}).collect(Collectors.toList());

		} else {
			return keys;
		}
	}

}
