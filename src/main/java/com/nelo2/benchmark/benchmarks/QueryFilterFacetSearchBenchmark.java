/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.nelo2.benchmark.benchmarks;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.concurrent.ThreadLocalRandom;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.facet.FacetBuilder;
import org.elasticsearch.search.facet.FacetBuilders;

import com.nelo2.benchmark.Abstractbenchmark;

public class QueryFilterFacetSearchBenchmark extends Abstractbenchmark {

	static long COUNT = SizeValue.parseSizeValue("1m").singles();
	static int QUERY_COUNT = 200;

	static Client client;

	public static void main(String[] args) throws Exception {

	}

	private String[] fields;
	private String[] types;

	@Override
	public String name() {
		return "queryfilterfacet";
	}

	@Override
	public boolean settings() {
		if (!super.settings()) {
			return false;
		}

		fields = componentSettings.getAsArray(".fields");
		types = componentSettings.getAsArray(".types");
		if (fields == null) {
			err("fields missing...");
			return false;
		}

		if (fields.length != types.length) {
			err("can not match all the fields to filter type...");
			return false;
		}
		return true;
	}

	@Override
	public void benchmark() {
		if (!settings())
			return;

		long totalQueryTime = 0;

		totalQueryTime = 0;

		SearchRequestBuilder builder = client.prepareSearch();

		for (int j = 0; j < QUERY_COUNT; j++) {
			SearchResponse searchResponse = client.prepareSearch().setSearchType(SearchType.COUNT)
					.setQuery(termQuery("l_value", lValues[0])).execute().actionGet();
			totalQueryTime += searchResponse.getTookInMillis();
		}
		log(" Simple Query on first l_value " + (totalQueryTime / QUERY_COUNT) + "ms");

		totalQueryTime = 0;
		for (int j = 0; j < QUERY_COUNT; j++) {
			SearchResponse searchResponse = client.prepareSearch().setSearchType(SearchType.COUNT)
					.setQuery(termQuery("l_value", lValues[0]))
					.addFacet(FacetBuilders.queryFacet("query").query(termQuery("l_value", lValues[0]))).execute()
					.actionGet();
			totalQueryTime += searchResponse.getTookInMillis();
		}
		log(" Query facet first l_value " + (totalQueryTime / QUERY_COUNT) + "ms");

		totalQueryTime = 0;
		for (int j = 0; j < QUERY_COUNT; j++) {
			SearchResponse searchResponse = client
					.prepareSearch()
					.setSearchType(SearchType.COUNT)
					.setQuery(termQuery("l_value", lValues[0]))
					.addFacet(
							FacetBuilders.queryFacet("query").query(termQuery("l_value", lValues[0])).global(true)
									.mode(FacetBuilder.Mode.COLLECTOR)).execute().actionGet();
			totalQueryTime += searchResponse.getTookInMillis();
		}
		log(" Query facet first l_value (global) (mode/collector) " + (totalQueryTime / QUERY_COUNT) + "ms");

		totalQueryTime = 0;
		for (int j = 0; j < QUERY_COUNT; j++) {
			SearchResponse searchResponse = client
					.prepareSearch()
					.setSearchType(SearchType.COUNT)
					.setQuery(termQuery("l_value", lValues[0]))
					.addFacet(
							FacetBuilders.queryFacet("query").query(termQuery("l_value", lValues[0])).global(true)
									.mode(FacetBuilder.Mode.POST)).execute().actionGet();
			totalQueryTime += searchResponse.getTookInMillis();
		}
		log(" Query facet first l_value (global) (mode/post) " + (totalQueryTime / QUERY_COUNT) + "ms");
	}

	public SearchRequestBuilder requestBuild(String field,SearchRequestBuilder builder, Settings settings) {

		String type = settings.get("type");
		if(type == null || "".equals(type)){
			err(field+" type is missing...");
			return null;
		}
		
		
		return builder;
	}
}
