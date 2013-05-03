package com.nelo2.benchmark.benchmarks;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.RandomStringGenerator;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;

import com.nelo2.benchmark.Abstractbenchmark;

public class IndexBenchmark extends Abstractbenchmark {

	static String indexName = "nelo2-log-2013-05-02";
	static long SIZE = SizeValue.parseSizeValue("20k").singles();

	static int PROJECT_COUNT = 1200;
	static int BODY_COUNT = 1000;
	static int HOST_COUNT = 5000;
	static int WORD_COUNT = 10000;
	static long START_TIME = -1;
	static long END_TIME = Integer.MAX_VALUE;
	static int APP_COUNT = 4000;
	static int APP_NAME_SIZE = 10;

	static String[] LOG_LEVEL = { "DEBUG", "ERROR", "INFO", "WARNING", "FATAL" };
	static int LOG_LEVEL_COUNT = LOG_LEVEL.length;

	static int BATCH = 100;

	private static boolean deleteIndex = true;

	public static void main(String[] args) throws InterruptedException {
		IndexBenchmark benchmark = new IndexBenchmark();
		benchmark.benchmark();
	}

	private static String getBody(String[] words, String[] temp) {
		StringBuilder sb = new StringBuilder(1000 + 100);
		sb.append(words[ThreadLocalRandom.current().nextInt(0, words.length)]);
		for (int i = 1; i < 100; i++) {
			sb.append(" ").append(words[ThreadLocalRandom.current().nextInt(0, words.length)]);
		}
		return sb.toString();
	}

	private Random randomTime;

	@Override
	public String name() {
		return "index";
	}

	@Override
	public boolean settings() {
		super.settings();

		String size = settings.get("size", "20m");
		SIZE = SizeValue.parseSizeValue(size).singles();

		Settings benchSettings = settings.getByPrefix(name());
		BATCH = benchSettings.getAsInt(".batch", 500);
		LOG_LEVEL = benchSettings.getAsArray(".log.levels");
		if (LOG_LEVEL == null || LOG_LEVEL.length == 0) {
			err(name() + ".log.levels is empty");
			return false;
		}
		LOG_LEVEL_COUNT = LOG_LEVEL.length;

		PROJECT_COUNT = benchSettings.getAsInt(".project.size", 1200);
		BODY_COUNT = benchSettings.getAsInt(".body.size", 2000);
		HOST_COUNT = benchSettings.getAsInt(".host.size", 5000);
		WORD_COUNT = benchSettings.getAsInt(".word.size", 10000);

		deleteIndex = benchSettings.getAsBoolean(".delete", false);

		END_TIME = SizeValue.parseSizeValue(benchSettings.get(".logtime.end", "36m")).singles();
		randomTime =  new Random(END_TIME);
		return true;
	}

	@Override
	public void benchmark() {
		if (!settings())
			return;
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

		log("data prepared finished");
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			client.admin().indices().create(Requests.createIndexRequest(indexName)).actionGet();
		} catch (Exception e) {
			err(indexName + " already exists..");
			if (deleteIndex) {
				log("need delete the '" + indexName + "'");
				client.admin().indices().delete(Requests.deleteIndexRequest(indexName)).actionGet();
				log("deleted...");
			}
		}
		try {
			StopWatch stopWatch = new StopWatch().start();

			log("Indexing [" + SIZE + "] ...");
			int count = 0;
			BulkRequestBuilder builder = client.prepareBulk();
			Map<String, Object> map = null;
			IndexRequest request = null;
			String[] temp = new String[WORD_COUNT];
			while (count < SIZE) {
				request = Requests.indexRequest();
				map = new HashMap<String, Object>(5);
				long start = System.currentTimeMillis();
				String projectName = projectNames[random.nextInt(projectNames.length)];
				String body = getBody(words, temp);
				map.put("projectName", projectName);
				map.put("logTime", randomTime.nextLong());
				map.put("logLevel", LOG_LEVEL[random.nextInt(LOG_LEVEL_COUNT)]);
				map.put("body", body);
				map.put("host", hosts[random.nextInt(HOST_COUNT)]);
				request.type(projectName).index(indexName).source(map);
				builder.add(request);
				count++;
				if (builder.numberOfActions() == BATCH) {
					BulkResponse response = builder.execute().actionGet();
					if (response.hasFailures()) {
						err("there are failed request!");
					}
					builder = client.prepareBulk();
					if ((count % (BATCH * 10)) == 0) {
						log("Indexed " + count + " took " + stopWatch.stop().lastTaskTime());
						stopWatch.start();
					}
				}
			}
			log("Indexing took " + stopWatch.totalTime() + ", TPS "
					+ (((double) (SIZE)) / stopWatch.totalTime().secondsFrac()));
		} catch (Exception e) {
			err("Index already exists, ignoring indexing phase, waiting for green");
			ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth()
					.setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
			if (clusterHealthResponse.isTimedOut()) {
				log("Timed out waiting for cluster health");
			}
		}
		client.admin().indices().prepareRefresh().execute().actionGet();
		SIZE = client.prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount();
		log("--> Number of docs in index: " + SIZE);
	}
}
