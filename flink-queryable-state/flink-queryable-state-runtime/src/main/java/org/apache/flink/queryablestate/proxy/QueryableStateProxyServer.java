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

package org.apache.flink.queryablestate.proxy;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.queryablestate.network.stats.AtomicKvStateRequestStats;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.query.QueryableStateProxyService;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskexecutor.QueryableStateConfiguration;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

public class QueryableStateProxyServer {

	private static final Logger LOG = LoggerFactory.getLogger(QueryableStateProxyServer.class);

	public static void main(String[] args) throws Exception {
		ParameterTool pt = ParameterTool.fromArgs(args);
		String configDir = pt.getRequired("configDir");

		LOG.info("Loading configuration from {}", configDir);
		final Configuration flinkConfig = GlobalConfiguration.loadConfiguration(configDir);
		final QueryableStateConfiguration queryableConfig = QueryableStateConfiguration.fromConfiguration(flinkConfig);

		QueryableStateProxyService proxyService = new QueryableStateProxyServiceImpl(
			InetAddress.getByName(queryableConfig.getProxyMetaHost()),
			queryableConfig.getProxyPortRange(),
			queryableConfig.numProxyMetaServerThreads(),
			queryableConfig.numProxyMetaQueryThreads(),
			new AtomicKvStateRequestStats()
		);

		try {
			HighAvailabilityServices haService = createHaServices(flinkConfig);
			proxyService.setHighAvailabilityServices(haService);

			proxyService.start();
		} catch (Throwable e) {
			e.printStackTrace();
		}


		// run the history server
		SecurityUtils.install(new SecurityConfiguration(flinkConfig));

		try {
			SecurityUtils.getInstalledContext().runSecured(new Callable<Integer>() {
				@Override
				public Integer call() throws Exception {
					try {
						proxyService.start();
					} catch (Throwable throwable) {
						throwable.printStackTrace();
					}
					return 0;
				}
			});
			proxyService.shutdown();
			System.exit(0);
		} catch (Throwable t) {
			final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
			LOG.error("Failed to run QueryableStateProxyServer.", strippedThrowable);
			strippedThrowable.printStackTrace();
			System.exit(1);
		}

	}

	private static HighAvailabilityServices createHaServices(Configuration configuration) throws Exception {
		return HighAvailabilityServicesUtils.createHighAvailabilityServices(
			configuration,
			Executors.directExecutor(),
			HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION);
	}

}
