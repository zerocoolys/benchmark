/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.nelo2.benchmark.benchmarks;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.facet.FacetBuilders.termsFacet;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;

import com.nelo2.benchmark.Abstractbenchmark;

/**
 *
 */
public class TermsFacetSearchBenchmark extends Abstractbenchmark {

	long COUNT = SizeValue.parseSizeValue("20k").singles();
	int QUERY_WARMUP = 50;
	int QUERY_COUNT = 500;
	String[] fields = null;
	private String executionHint;

	public static void main(String[] args) throws Exception {
		TermsFacetSearchBenchmark benchmark = new TermsFacetSearchBenchmark();
		benchmark.benchmark();
	}

	static class StatsResult {
		final String name;
		final long took;

		StatsResult(String name, long took) {
			this.name = name;
			this.took = took;
		}
	}

	private StatsResult terms(String name, String field, String executionHint) {
		long totalQueryTime;

		client.admin().indices().prepareClearCache().setFieldDataCache(true).execute().actionGet();

		log("Warmup (" + name + ")...");
		SearchRequestBuilder builder = client.prepareSearch().setSearchType(SearchType.COUNT).setQuery(matchAllQuery())
				.addFacet(termsFacet(field).field(field).executionHint(executionHint));

		SearchRequest request = builder.request();
		logRequest(request);

		for (int j = 0; j < QUERY_WARMUP; j++) {
			SearchResponse searchResponse = client.search(request).actionGet();
			if (j == 0) {
				log("Loading (" + field + "): took: " + searchResponse.getTook());
			}
			if (searchResponse.getHits().totalHits() != COUNT) {
				err("mismatch on hits");
			}
		}
		log("Warmup (" + name + ") DONE");

		log("Running (" + name + ")...");
		totalQueryTime = 0;
		for (int j = 0; j < QUERY_COUNT; j++) {
			SearchResponse searchResponse = client.prepareSearch().setSearchType(SearchType.COUNT)
					.setQuery(matchAllQuery()).addFacet(termsFacet(field).field(field).executionHint(executionHint))
					.execute().actionGet();
			if (searchResponse.getHits().totalHits() != COUNT) {
				err("mismatch on hits");
			}
			totalQueryTime += searchResponse.getTookInMillis();
		}
		log("Terms Facet (" + field + "), hint(" + executionHint + "): " + (totalQueryTime / QUERY_COUNT) + "ms");
		return new StatsResult(name, totalQueryTime);
	}

	@Override
	public String name() {
		return "termsfacet";
	}

	@Override
	public boolean settings() {
		if (!super.settings()) {
			return false;
		}
		Settings groupSettings = settings.getByPrefix(name());
		COUNT = SizeValue.parseSizeValue(settings.get("size")).singles();
		QUERY_COUNT = groupSettings.getAsInt(".query.count", 500);
		QUERY_WARMUP = groupSettings.getAsInt(".query.warmup", 500);

		fields = groupSettings.getAsArray(".fields");
		executionHint = groupSettings.get(".executionHint");
		return true;
	}

	@Override
	public void benchmark() {
		if (!settings())
			return;

		List<StatsResult> stats = new ArrayList<StatsResult>();
		for (String field : fields) {
			stats.add(terms("terms_" + field, field, null));
		}
		//		stats.add(terms("terms_map_projectName", "projectName", "map"));
		//		stats.add(terms("terms_host", "host", null));
		//		stats.add(terms("terms_map_host", "host", "map"));
		//		stats.add(terms("terms_logLevel", "logLevel", null));
		//		stats.add(terms("terms_map_logLevel", "logLevel", "map"));
		//		stats.add(terms("terms_body", "body", null));
		//		stats.add(terms("terms_map_body", "body", "map"));

		System.out.println("------------------ SUMMARY -------------------------------");
		System.out.format("%25s%10s%10s\n", "name", "took", "millis");
		for (StatsResult stat : stats) {
			System.out.format("%25s%10s%10d\n", stat.name, TimeValue.timeValueMillis(stat.took),
					(stat.took / QUERY_COUNT));
		}
		System.out.println("------------------ SUMMARY -------------------------------");

		client.close();
	}
}
