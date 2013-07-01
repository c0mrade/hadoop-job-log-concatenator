package hadoop.log.util;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.log4j.Logger;

public class Concatenator {

	private static final Logger LOGGER;
	private static final String HTTP_PROTOCOL_PREFIX = "http://";
	private static final String DEFAULT_TASK_TRACKER_PORT = "50060";
	private static final String USER_LOG_RESOURCE = "/logs/userlogs/";
	private static final String ZOOKEPPER_QUORUM = "zookeeper.quorum";
	private static final String HADOOP_JOB_ID = "hadoop.job.id";
	private static final String OUTPUT_LOG_FILE = "output.log.file";
	private static final String MAPRED_JOB_TRACKER = "mapred.job.tracker";
	private static final String COMPRESS_OUTPUT = "compress.output";

	static {
		LOGGER = Logger.getLogger(hadoop.log.util.Concatenator.class);
	}

	public static void main(String[] args) throws Exception {
		Concatenator cc = new Concatenator();
		cc.validateConfiguration();
		cc.run();
	}
	
	public Concatenator() {
	}

	private void usage() {
		LOGGER.info("Usage > hadoop jar <jar-file> -Dhadoop.job.id=<job-id> -Dzookeeper.quorum=<zookeeper-quorum> -Dmapred.job.tracker=<mapred-jobtracker> -Doutput.log.file=<output-file> [ -Dcompress.output=<true|false default false> -Dhadoop.mapreduce.tasktracker.port=<port default 50060>");
		System.exit(0);
	}
	
	private void validateConfiguration() { 
		if (System.getProperty(HADOOP_JOB_ID) == null) {
			LOGGER.error("Missing hadoop.job.id property in configuration");
			usage();
		}
		if (System.getProperty(OUTPUT_LOG_FILE) == null) {
			usage();
		}
		if(System.getProperty(ZOOKEPPER_QUORUM) == null){
			LOGGER.error("Missing zookeeper.quorum property in configuration");
			usage();
		}
		
		if(System.getProperty(MAPRED_JOB_TRACKER) == null){
			LOGGER.error("Missing mapred.job.tracker property in configuration");
			usage();
		}
	}

	private void run() throws Exception {
		long startTimeSt = System.currentTimeMillis();
		
		Configuration conf = new Configuration();

		conf.set("hbase.zookeeper.quorum", System.getProperty(ZOOKEPPER_QUORUM));
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		InetSocketAddress jobtracker = new InetSocketAddress(System.getProperty(MAPRED_JOB_TRACKER), 8021);
		
		String taskTrackerPort = DEFAULT_TASK_TRACKER_PORT;
		String outputFile = System.getProperty(OUTPUT_LOG_FILE);
		Boolean compressOutput = Boolean.valueOf(System.getProperty(COMPRESS_OUTPUT))==null;
		JobClient client = new JobClient(jobtracker, conf);
		client.setConf(conf);
		String jobId = System.getProperty(HADOOP_JOB_ID);
		List<String> taskTrackerNodes = getTaskTrackerNodes(client);
		OutputStream outputStream;
		if (compressOutput.booleanValue())
			outputStream = new BZip2CompressorOutputStream(new FileOutputStream(outputFile));
		else
			outputStream = new FileOutputStream(outputFile);
		for (Iterator<String> iterator = taskTrackerNodes.iterator(); iterator.hasNext();) {
			String taskTrackerNode = (String) iterator.next();
			String jobLogURL = HTTP_PROTOCOL_PREFIX.concat(taskTrackerNode).concat(":").concat(taskTrackerPort).concat(USER_LOG_RESOURCE).concat(jobId);
			String dataNodeURL = HTTP_PROTOCOL_PREFIX.concat(taskTrackerNode).concat(":").concat(taskTrackerPort);
			Iterator<String> it = getTaskAttemptsForNode(jobLogURL).iterator();
			while (it.hasNext()) {
				String taskAttempt = (String) it.next();
				String taskAttemptLogURL = dataNodeURL.concat("/").concat("tasklog?attemptid=").concat(taskAttempt).concat("&all=true");
				LOGGER.info((new StringBuilder()).append("Downloading file ").append(taskAttemptLogURL).toString());
				URL url = new URL(taskAttemptLogURL);
				HttpURLConnection connection = (HttpURLConnection) url.openConnection();
				connection.setConnectTimeout(2);
				connection.setReadTimeout(2);
				InputStream logInputStream = url.openStream();
				IOUtils.copy(logInputStream, outputStream);
				logInputStream.close();
				outputStream.flush();
			}
		}

		outputStream.close();
		LOGGER.info((new StringBuilder()).append("Download complete in ").append(System.currentTimeMillis() - startTimeSt).append("ms").toString());
	}
	
	public List<String> getTaskAttemptsForNode(String surl) throws IOException {
		URL url = new URL(surl);
		List<String> attemptList = new ArrayList<String>();
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
			do {
				String inputLine;
				if ((inputLine = in.readLine()) == null)
					break;
				Pattern p = Pattern.compile("(attempt_\\d+_\\d+_[m|r]_\\d+_\\d+)");
				Matcher m = p.matcher(inputLine);
				if (m.find())
					attemptList.add(m.group(1));
			} while (true);
			in.close();
		} catch (IOException e) {
			LOGGER.debug((new StringBuilder()).append("No data at URL ").append(url).toString());
		}
		return attemptList;
	}

	private List<String> getTaskTrackerNodes(JobClient client) throws IOException {
		ClusterStatus status = client.getClusterStatus(true);
		List<String> taskTrackerNodes = new ArrayList<String>(status.getActiveTrackerNames().size());
		String a;
		for (Iterator<String> it = status.getActiveTrackerNames().iterator(); it.hasNext(); taskTrackerNodes.add(a.replace("tracker_", "").split(":")[0]))
			a = (String) it.next();

		return taskTrackerNodes;
	}

}
