package com.nelo2.benchmark;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public abstract class Abstractbenchmark {

	protected static String index;
	protected static String type;
	protected static String routing;
	protected static Client client;
	protected Settings settings;
	protected Settings componentSettings;
	protected boolean DEBUG = false;

	public abstract String name();

	public boolean settings() {
		this.settings = ImmutableSettings.settingsBuilder().loadFromClasspath(".\\benchmark.yml").build();
		componentSettings = this.settings.getByPrefix(name());
		
		DEBUG = settings.getAsBoolean("debug", false);
		Settings temp = ImmutableSettings.settingsBuilder().put("cluster.name", settings.get("cluster.name")).build();
		client = new TransportClient(temp).addTransportAddress(new InetSocketTransportAddress(settings.get("ip"),
				settings.getAsInt("port", 9300)));
		return true;
	}

	public abstract void benchmark();

	public static void usage() {
		System.out.println("-----------------");
		System.out.println(" -i|-index [indexname]");
		System.out.println(" -t|-type [typename]");
		System.out.println(" -r|-routing [routing]");
	}

	public static boolean parseArgs(String[] args) {
		if (args.length % 2 != 0) {
			usage();
			return false;
		}
		for (int i = 0; i < args.length; i = +2) {
			if (args[i].equals("-i") || args[i].equals("-index")) {
				index = args[i + 1];
				continue;
			} else if (args[i].equals("-t") || args[i].equals("-type")) {
				type = args[i + 1];
				continue;
			} else if (args[i].equals("-r") || args[i].equals("-routing")) {
				routing = args[i + 1];
				continue;
			}
		}
		return true;
	}

	private String[] benchmarkNames = null;

	public boolean validateSettings() {
		benchmarkNames = settings.get("benchmark.names").split(",");

		if (benchmarkNames == null || benchmarkNames.length == 0) {
			System.out.println("benchmark.names is empty");
			return false;
		}
		return true;
	}

	protected static void log(String str) {
		System.out.println("--> " + str);
	}

	protected static void err(String str) {
		System.err.println("--> " + str);
	}

	protected void logRequest(ActionRequest request) {
		if (DEBUG) {
			log("request content " + request.toString());
		}
	}
}
