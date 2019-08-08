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

import org.apache.flink.queryablestate.messages.KvStateLocationRequest;
import org.apache.flink.queryablestate.messages.KvStateLocationResponse;
import org.apache.flink.queryablestate.network.AbstractServerBase;
import org.apache.flink.queryablestate.network.AbstractServerHandler;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;
import org.apache.flink.queryablestate.network.stats.KvStateRequestStats;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.query.QueryableStateProxyService;
import org.apache.flink.util.Preconditions;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * The implementation of {@link QueryableStateProxyService}
 */
public class QueryableStateProxyServiceImpl extends AbstractServerBase<KvStateLocationRequest, KvStateLocationResponse> implements QueryableStateProxyService {

	private final KvStateRequestStats stats;

	private final int queryExecutorThreads;

	private HighAvailabilityServices highAvailabilityServices;

	private DispatcherGateway dispatcherGateway;

	public QueryableStateProxyServiceImpl(
		final InetAddress bindAddress,
		final Iterator<Integer> bindPortIterator,
		final Integer numEventLoopThreads,
		final Integer numQueryThreads,
		final KvStateRequestStats stats) {

		super("Queryable State Proxy Service", bindAddress, bindPortIterator, numEventLoopThreads, numQueryThreads);
		Preconditions.checkArgument(numQueryThreads >= 1, "Non-positive number of query threads.");
		this.queryExecutorThreads = numQueryThreads;
		this.stats = Preconditions.checkNotNull(stats);
	}

	@Override
	public void setHighAvailabilityServices(HighAvailabilityServices haService) {
		this.highAvailabilityServices = haService;
	}

	@Override
	public HighAvailabilityServices getHighAvailabilityServices() {
		return this.highAvailabilityServices;
	}

	@Override
	public void setDispatcherGateway(DispatcherGateway dispatcherGateway) {
		this.dispatcherGateway = dispatcherGateway;
	}

	@Override
	public DispatcherGateway getDispatcherGateway() {
		return this.dispatcherGateway;
	}

	@Override
	public AbstractServerHandler<KvStateLocationRequest, KvStateLocationResponse> initializeHandler() {
		MessageSerializer<KvStateLocationRequest, KvStateLocationResponse> serializer =
			new MessageSerializer<>(
				new KvStateLocationRequest.KvStateLocationRequestDeserializer(),
				new KvStateLocationResponse.KvStateLocationResponseDeserializer());
		return new QueryableStateProxyServiceHandler(this, serializer, stats);
	}

	@Override
	public void shutdown() {
		try {
			shutdownServer().get(10L, TimeUnit.SECONDS);
			log.info("{} was shutdown successfully.", getServerName());
		} catch (Exception e) {
			log.warn("{} shutdown failed: {}", getServerName(), e);
		}
	}
}
