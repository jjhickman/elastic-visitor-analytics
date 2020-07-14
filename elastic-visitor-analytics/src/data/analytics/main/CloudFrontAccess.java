package data.analytics.main;

import data.analytics.constants.Constants;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import data.analytics.mapreduce.VisitorMapper;
import data.analytics.mapreduce.VisitorReducer;

public class CloudFrontAccess extends Configured implements Tool {

	private static final String JOB_NAME = "Calculating hits by country and by IP";

	@Override
	public int run(String[] args) throws Exception {

		try {
			Configuration config = getConf();
			config.setBoolean("mapred.output.compress", true);
			config.set("mapred.output.compression.type", "BLOCK");
			config.set("mapred.output.compression.codec","org.apache.hadoop.io.compress.GzipCodec");
			config.set("mapred.textoutputformat.separator", ",");

			Job job = Job.getInstance(config);

			job.setJarByClass(CloudFrontAccess.class);
			job.setJobName(JOB_NAME);

			job.setMapperClass(VisitorMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(VisitorReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.setInputPaths(job, args[0]);
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			MultipleOutputs.addNamedOutput(job, Constants.DETAILS_BY_CITY_COUNTRY_TABLE_NAME, TextOutputFormat.class,
					Text.class, Text.class);
			MultipleOutputs.setCountersEnabled(job, true);

			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
			boolean success = job.waitForCompletion(true);
			return success ? 0 : 1;
		} catch (Exception e) {
			e.printStackTrace();
			return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Usage: CloudFrontAccess <input directories> <output directory>");
			System.exit(-1);
		}
		int result = ToolRunner.run(new CloudFrontAccess(), args);
		System.exit(result);
	}
}
