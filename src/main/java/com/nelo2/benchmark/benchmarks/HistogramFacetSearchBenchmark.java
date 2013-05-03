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
import static org.elasticsearch.search.facet.FacetBuilders.dateHistogramFacet;
import static org.elasticsearch.search.facet.FacetBuilders.histogramFacet;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.search.facet.FacetBuilder;

import com.nelo2.benchmark.Abstractbenchmark;

/**
 *
 */
public class HistogramFacetSearchBenchmark extends Abstractbenchmark {

	public static void main(String[] args) throws Exception {
		HistogramFacetSearchBenchmark benchmark = new HistogramFacetSearchBenchmark();
		benchmark.benchmark();

	}

	private int QUERY_COUNT;
	private long COUNT;
	private int QUERY_WARMUP;

	@Override
	public String name() {
		return "histogramfacet";
	}

	@Override
	public boolean settings() {
		if (!super.settings()) {
			return false;
		}
		COUNT = SizeValue.parseSizeValue(settings.get("size", "2m")).singles();

		Settings groupSetting = settings.getByPrefix(name());
		
		QUERY_COUNT = groupSetting.getAsInt(".query.count", settings.getAsInt("query.count", 500));
		QUERY_WARMUP = groupSetting.getAsInt(".warnup.count", settings.getAsInt("warnup.count", 50));
		
		return true;
	}

	@Override
	public void benchmark() {
		if (!settings())
			return;

		log("Warmup...");
		// run just the child query, warm up first
		for (int j = 0; j < QUERY_WARMUP; j++) {
			SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery())
					.addFacet(histogramFacet("l_value").field("l_value").interval(4))
					.addFacet(histogramFacet("date").field("date").interval(1000)).execute().actionGet();
			if (j == 0) {
				log("Warmup took: " + searchResponse.getTook());
			}
			if (searchResponse.getHits().totalHits() != COUNT) {
				err("mismatch on hits");
			}
		}
		log("Warmup DONE");

		long totalQueryTime = 0;
		for (int j = 0; j < QUERY_COUNT; j++) {
			SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery())
					.addFacet(histogramFacet("l_value").field("l_value").interval(4)).execute().actionGet();
			if (searchResponse.getHits().totalHits() != COUNT) {
				err("mismatch on hits");
			}
			totalQueryTime += searchResponse.getTookInMillis();
		}
		log("Histogram Facet (l_value) " + (totalQueryTime / QUERY_COUNT) + "ms");

		totalQueryTime = 0;
		for (int j = 0; j < QUERY_COUNT; j++) {
			SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery())
					.addFacet(histogramFacet("l_value").field("l_value").valueField("l_value").interval(4)).execute()
					.actionGet();
			if (searchResponse.getHits().totalHits() != COUNT) {
				err("mismatch on hits");
			}
			totalQueryTime += searchResponse.getTookInMillis();
		}
		log("Histogram Facet (l_value/l_value) " + (totalQueryTime / QUERY_COUNT) + "ms");

		totalQueryTime = 0;
		for (int j = 0; j < QUERY_COUNT; j++) {
			SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery())
					.addFacet(histogramFacet("date").field("date").interval(1000)).execute().actionGet();
			if (searchResponse.getHits().totalHits() != COUNT) {
				err("mismatch on hits");
			}
			totalQueryTime += searchResponse.getTookInMillis();
		}
		log("Histogram Facet (date) " + (totalQueryTime / QUERY_COUNT) + "ms");

		totalQueryTime = 0;
		for (int j = 0; j < QUERY_COUNT; j++) {
			SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery())
					.addFacet(histogramFacet("date").field("date").valueField("l_value").interval(1000)).execute()
					.actionGet();
			if (searchResponse.getHits().totalHits() != COUNT) {
				err("mismatch on hits");
			}
			totalQueryTime += searchResponse.getTookInMillis();
		}
		log("Histogram Facet (date/l_value) " + (totalQueryTime / QUERY_COUNT) + "ms");

		totalQueryTime = 0;
		for (int j = 0; j < QUERY_COUNT; j++) {
			SearchResponse searchResponse = client
					.prepareSearch()
					.setQuery(matchAllQuery())
					.addFacet(
							dateHistogramFacet("date").field("date").interval("day").mode(FacetBuilder.Mode.COLLECTOR))
					.execute().actionGet();
			if (searchResponse.getHits().totalHits() != COUNT) {
				err("mismatch on hits");
			}
			totalQueryTime += searchResponse.getTookInMillis();
		}
		log("Date Histogram Facet (mode/collector) (date) " + (totalQueryTime / QUERY_COUNT) + "ms");

		totalQueryTime = 0;
		for (int j = 0; j < QUERY_COUNT; j++) {
			SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery())
					.addFacet(dateHistogramFacet("date").field("date").interval("day").mode(FacetBuilder.Mode.POST))
					.execute().actionGet();
			if (searchResponse.getHits().totalHits() != COUNT) {
				err("mismatch on hits");
			}
			totalQueryTime += searchResponse.getTookInMillis();
		}
		log("Date Histogram Facet (mode/post) (date) " + (totalQueryTime / QUERY_COUNT) + "ms");

	}
}
