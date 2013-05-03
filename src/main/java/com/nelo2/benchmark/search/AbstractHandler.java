package com.nelo2.benchmark.search;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.common.settings.Settings;

public abstract class AbstractHandler<T extends ActionRequestBuilder> {

	abstract String name();
	
	abstract void handle(Settings settings, T builder);
		
}
