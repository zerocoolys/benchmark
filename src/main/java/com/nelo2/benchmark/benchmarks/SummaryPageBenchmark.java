package com.nelo2.benchmark.benchmarks;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.RandomStringGenerator;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.nelo2.benchmark.Abstractbenchmark;

public class SummaryPageBenchmark extends Abstractbenchmark {

	static String indexName = "nelo2-log-2013-05-02";
	static long COUNT = SizeValue.parseSizeValue("20m").singles();
	static int PROJECT_COUNT = 1200;
	static int BODY_COUNT = 1000;
	static int HOST_COUNT = 5000;
	static int WORD_COUNT = 10000;
	static String[] LOG_LEVEL = { "DEBUG", "ERROR", "INFO", "WARNING", "FATAL" };
	static int LOG_LEVEL_COUNT = LOG_LEVEL.length;
	static int BATCH = 100;
	static int QUERY_WARMUP = 20;
	static int QUERY_COUNT = 200;
	static int NUMBER_OF_TERMS = 200;
	static int NUMBER_OF_MULTI_VALUE_TERMS = 10;
	static int STRING_TERM_SIZE = 5;
	private static boolean deleteIndex = false;

	private static void createProjectIndex(Client client) {
		String[] projectNames = new String[PROJECT_COUNT];

		for (int i = 0; i < projectNames.length; i++) {
			projectNames[i] = RandomStringGenerator.randomAlphabetic(15);
		}

		String[] words = new String[WORD_COUNT];
		for (int i = 0; i < WORD_COUNT; i++) {
			words[i] = RandomStringGenerator.random(10, true, false);
		}

		String[] bodys = new String[BODY_COUNT];
		for (int i = 0; i < bodys.length; i++) {
			bodys[i] = RandomStringGenerator.random(5000, true, true);
		}

		long current = 1364900000000l;

		int duration = 24 * 60 * 60 * 1000;

		// random host lists
		String[] hosts = new String[HOST_COUNT];
		Random random = new Random();
		for (int i = 0; i < hosts.length; i++) {
			int a = random.nextInt(256);
			int b = random.nextInt(256);
			int c = random.nextInt(256);
			int d = random.nextInt(256);
			hosts[i] = a + "." + b + "." + c + "." + d;
		}
		try {
			client.admin().indices().create(Requests.createIndexRequest(indexName)).actionGet();
		} catch (Exception e) {
			e.printStackTrace();
			if (deleteIndex)
				client.admin().indices().delete(Requests.deleteIndexRequest(indexName)).actionGet();
		}
		try {

			int count = 0;
			BulkRequestBuilder builder = client.prepareBulk();
			Map<String, Object> map = null;
			IndexRequest request = null;
			while (count < COUNT) {
				request = Requests.indexRequest();
				map = new HashMap<String, Object>(5);
				long start = System.currentTimeMillis();
				String projectName = projectNames[random.nextInt(projectNames.length)];
				String body = getBody(words);
				map.put("projectName", projectName);
				map.put("logTime", random.nextInt(duration) + current);
				map.put("logLevel", LOG_LEVEL[random.nextInt(LOG_LEVEL_COUNT)]);
				map.put("body", body);
				map.put("host", hosts[random.nextInt(HOST_COUNT)]);	
				request.type(projectName).index(indexName).source(map);
				//				LOG_LEVEL[random.nextInt(LOG_LEVEL_COUNT)], "body", bodys[random.nextInt(bodys.length)]);
				builder.add(request);
				count++;
				if (builder.numberOfActions() == BATCH) {
					BulkResponse response = builder.execute().actionGet();
					if (response.hasFailures()) {
						System.err.println("there are failed request!");
					}
					builder = client.prepareBulk();
					System.out.println("batch " + count + " :" + (System.currentTimeMillis() - start));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	static String[] temp = new String[100];

	private static String getBody(String[] words) {
		StringBuilder sb = new StringBuilder(1000);
		for (int i = 0; i < 100; i++) {
			temp[i] = words[ThreadLocalRandom.current().nextInt(0, words.length)];
		}
		return Arrays.toString(temp);
	}

	public static void main(String[] args) throws InterruptedException {

		//		ModulesBuilder builder = new ModulesBuilder();
		//		builder.add(new SearchModule());
		//		
		//		Injector injector = builder.createInjector();
		//		injector.getInstance(Abstractbenchmark.class);

		if (!parseArgs(args)) {
			usage();
			return;
		}
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch-20.6").build();
		TransportClient client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(
				"10.34.130.201", 9300));
		createProjectIndex(client);
		// prepareIndex(client);

		//		SearchRequestBuilder requestBuilder;
		//		if (index == null) {
		//			requestBuilder = client.prepareSearch();
		//		} else {
		//			requestBuilder = client.prepareSearch(index.split(","));
		//		}
		//
		//		if (type != null && !"".equals(type))
		//			requestBuilder.setTypes(type.split(","));
		//
		//		if (routing != null && !"".equals(routing))
		//			requestBuilder.setRouting(routing);
		//
		//		requestBuilder.addFacet(terms());
		//		ListenableActionFuture<SearchResponse> future = requestBuilder.execute();
		//
		//		SearchResponse response = future.actionGet();
		//
		//		System.out.println(response.getTookInMillis());
		client.close();
	}

	private static void prepareIndex(Client client) {

		long[] lValues = new long[NUMBER_OF_TERMS];
		for (int i = 0; i < NUMBER_OF_TERMS; i++) {
			lValues[i] = ThreadLocalRandom.current().nextLong();
		}
		String[] sValues = new String[NUMBER_OF_TERMS];
		for (int i = 0; i < NUMBER_OF_TERMS; i++) {
			sValues[i] = RandomStringGenerator.randomAlphabetic(STRING_TERM_SIZE);
		}

		try {
			Thread.sleep(10000);
			client.admin().indices().create(createIndexRequest("test")).actionGet();

			StopWatch stopWatch = new StopWatch().start();

			log("Indexing [" + COUNT + "] ...");
			long ITERS = COUNT / BATCH;
			long i = 1;
			int counter = 0;
			for (; i <= ITERS; i++) {
				BulkRequestBuilder request = client.prepareBulk();
				for (int j = 0; j < BATCH; j++) {
					counter++;

					XContentBuilder builder = jsonBuilder().startObject();
					builder.field("id", Integer.toString(counter));
					builder.field("s_value", sValues[counter % sValues.length]);
					builder.field("l_value", lValues[counter % lValues.length]);

					builder.startArray("sm_value");
					for (int k = 0; k < NUMBER_OF_MULTI_VALUE_TERMS; k++) {
						builder.value(sValues[ThreadLocalRandom.current().nextInt(sValues.length)]);
					}
					builder.endArray();

					builder.startArray("lm_value");
					for (int k = 0; k < NUMBER_OF_MULTI_VALUE_TERMS; k++) {
						builder.value(lValues[ThreadLocalRandom.current().nextInt(sValues.length)]);
					}
					builder.endArray();

					builder.endObject();

					request.add(Requests.indexRequest("test").type("type1").id(Integer.toString(counter))
							.source(builder));
				}
				BulkResponse response = request.execute().actionGet();
				if (response.hasFailures()) {
					err("failures...");
				}
				if (((i * BATCH) % 10000) == 0) {
					log("Indexed " + (i * BATCH) + " took " + stopWatch.stop().lastTaskTime());
					stopWatch.start();
				}
			}
			log("Indexing took " + stopWatch.totalTime() + ", TPS "
					+ (((double) (COUNT)) / stopWatch.totalTime().secondsFrac()));
		} catch (Exception e) {
			log("Index already exists, ignoring indexing phase, waiting for green");
			ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth()
					.setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
			if (clusterHealthResponse.isTimedOut()) {
				err("Timed out waiting for cluster health");
			}
		}
		client.admin().indices().prepareRefresh().execute().actionGet();
		COUNT = client.prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount();
		log("Number of docs in index: " + COUNT);

	}

	@Override
	public String name() {
		return "summarypage";
	}

	@Override
	public void benchmark() {
		// TODO Auto-generated method stub
		
	}

	//	private static AbstractFacetBuilder terms() {
	//		TermsFacetBuilder tfbuilder = new TermsFacetBuilder("s_value");
	//		RangeFilterBuilder range = new RangeFilterBuilder("logTime");
	//		long current = System.currentTimeMillis();
	//		long last1day = current - 24 * 60 * 60 * 1000;
	//		range.from(last1day).to(current);
	//		tfbuilder.field("s_value").size(80);// .facetFilter(range);
	//		return tfbuilder;
	//	}

	//	private static FacetBuilder terms() {
	//		TermsFacetBuilder tfbuilder = new TermsFacetBuilder("s_value");
	//		RangeFilterBuilder range = new RangeFilterBuilder("logTime");
	//		long current = System.currentTimeMillis();
	//		long last1day = current - 24 * 60 * 60 * 1000;
	//		range.from(last1day).to(current);
	//		tfbuilder.field("s_value").size(80);// .facetFilter(range);
	//		return tfbuilder;
	//	}
}
