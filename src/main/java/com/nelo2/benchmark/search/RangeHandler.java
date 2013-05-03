package com.nelo2.benchmark.search;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.settings.Settings;

public class RangeHandler extends AbstractHandler<SearchRequestBuilder> {

	@Override
	String name() {
		return null;
	}

	@Override
	void handle(Settings settings, SearchRequestBuilder builder) {

	}

}
