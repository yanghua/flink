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

package org.apache.flink.queryablestate.locationservice;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.queryablestate.exceptions.UnknownKvStateIdException;
import org.apache.flink.queryablestate.exceptions.UnknownKvStateKeyGroupLocationException;
import org.apache.flink.queryablestate.exceptions.UnknownLocationException;
import org.apache.flink.queryablestate.messages.KvStateLocationRequest;
import org.apache.flink.queryablestate.messages.KvStateLocationResponse;
import org.apache.flink.queryablestate.network.AbstractServerHandler;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;
import org.apache.flink.queryablestate.network.stats.KvStateRequestStats;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * This handler receives the state location requests from clients than query state locations from cache,
 * if not available, then forward the request to dispatcher gateway.
 */
@Internal
@ChannelHandler.Sharable
public class QueryableStateLocationServiceHandler extends AbstractServerHandler<KvStateLocationRequest, KvStateLocationResponse> {

	private static final Logger LOG = LoggerFactory.getLogger(QueryableStateLocationServiceHandler.class);

	/** A cache to hold the location of different states for which we have already seen requests. */
	private final ConcurrentMap<Tuple2<JobID, String>, CompletableFuture<KvStateLocation>> lookupCache =
		new ConcurrentHashMap<>();

	private final QueryableStateLocationServiceImpl proxy;

	public QueryableStateLocationServiceHandler(
		final QueryableStateLocationServiceImpl proxy,
		MessageSerializer<KvStateLocationRequest, KvStateLocationResponse> serializer,
		KvStateRequestStats stats) {

		super(proxy, serializer, stats);
		this.proxy = proxy;
	}

	@Override
	public CompletableFuture<KvStateLocationResponse> handleRequest(long requestId, KvStateLocationRequest request) {
		CompletableFuture<KvStateLocationResponse> response = new CompletableFuture<>();
		executeActionAsync(response, request, false);
		return response;
	}

	private void executeActionAsync(
		final CompletableFuture<KvStateLocationResponse> result,
		final KvStateLocationRequest request,
		final boolean update) {

		if (!result.isDone()) {
			final CompletableFuture<KvStateLocationResponse> operationFuture = getStateLocation(request, update);
			operationFuture.whenCompleteAsync(
				(t, throwable) -> {
					if (throwable != null) {
						if (
							throwable.getCause() instanceof UnknownKvStateIdException ||
							throwable.getCause() instanceof UnknownKvStateKeyGroupLocationException ||
							throwable.getCause() instanceof ConnectException
							) {

							LOG.debug("Retrying after failing to retrieve state location due to: {}.", throwable.getCause().getMessage());
							executeActionAsync(result, request, true);
						} else {
							result.completeExceptionally(throwable);
						}
					} else {
						result.complete(t);
					}
				}, queryExecutor);

			result.whenComplete(
				(t, throwable) -> operationFuture.cancel(false));
		}

	}

	private CompletableFuture<KvStateLocationResponse> getStateLocation(
			final KvStateLocationRequest request,
			final boolean forceUpdate) {

		return getKvStateLocationLookupInfo(request.getJobId(), request.getQueryName(), forceUpdate)
			.thenComposeAsync((Function<KvStateLocation, CompletableFuture<KvStateLocationResponse>>) location -> {
				final int keyGroupIndex = KeyGroupRangeAssignment.computeKeyGroupForKeyHash(
					request.getKeyHashCode(), location.getNumKeyGroups());

				final InetSocketAddress serverAddress = location.getKvStateServerAddress(keyGroupIndex);
				final KvStateID kvStateId = location.getKvStateID(keyGroupIndex);
				if (serverAddress == null) {
					return FutureUtils.completedExceptionally(new UnknownKvStateKeyGroupLocationException(getServerName()));
				} else {
					return CompletableFuture.completedFuture(new KvStateLocationResponse(serverAddress, kvStateId));
				}
			}, queryExecutor);
	}

	private CompletableFuture<KvStateLocation> getKvStateLocationLookupInfo(
		final JobID jobId,
		final String queryableStateName,
		final boolean forceUpdate) {

		final Tuple2<JobID, String> cacheKey = new Tuple2<>(jobId, queryableStateName);
		final CompletableFuture<KvStateLocation> cachedFuture = lookupCache.get(cacheKey);

		if (!forceUpdate && cachedFuture != null && !cachedFuture.isCompletedExceptionally()) {
			LOG.debug("Retrieving location for state={} of job={} from the cache.", queryableStateName, jobId);
			return cachedFuture;
		}

		final DispatcherGateway gateway = proxy.getDispatcherGateway();

		if (gateway != null) {
			LOG.debug("Retrieving location for state={} of job={} from the key-value state location oracle.", queryableStateName, jobId);
			final CompletableFuture<KvStateLocation> location = new CompletableFuture<>();
			lookupCache.put(cacheKey, location);

			gateway.requestKvStateLocation(jobId, queryableStateName)
				.whenComplete(
					(KvStateLocation kvStateLocation, Throwable throwable) -> {
						if (throwable != null) {
							if (ExceptionUtils.stripCompletionException(throwable) instanceof FlinkJobNotFoundException) {
								lookupCache.remove(cacheKey);
							}
							location.completeExceptionally(throwable);
						} else {
							location.complete(kvStateLocation);
						}
					}
				);

			return location;
		} else {
			return FutureUtils.completedExceptionally(
				new UnknownLocationException(
					"Could not retrieve location of state=" + queryableStateName + " of job=" + jobId +
						". Potential reasons are: i) the state is not ready, or ii) the job does not exist."));
		}
	}

	@Override
	public CompletableFuture<Void> shutdown() {
		return CompletableFuture.completedFuture(null);
	}

}
