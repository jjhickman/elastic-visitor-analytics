package data.analytics.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import data.analytics.constants.Constants;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class VisitorReducer extends Reducer<Text, Text, Text, Text> {

	private Text outputValue = new Text();
	private MultipleOutputs<Text, Text> outputs;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		outputs = new MultipleOutputs<Text,Text>(context);
	}
	

	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Iterator<Text> iterator = values.iterator();
		Integer requests = 0;
		Integer misses = 0;
		Integer hits = 0;
		Integer errors = 0;
		Integer bytes = 0;
		String[] keyRows = key.toString().split(",");
		if(keyRows.length == 6) { // details by country
			while (iterator.hasNext()) {
				Text value = iterator.next();
				String[] rows = value.toString().split(",");
				requests += Integer.parseInt(rows[0]);
				hits += Integer.parseInt(rows[1]);
				misses += Integer.parseInt(rows[2]);
				errors += Integer.parseInt(rows[3]);
				bytes += Integer.parseInt(rows[4]);
			}
			outputValue.set(requests + "," + hits + "," + misses + ","
				+ errors + "," + bytes);
			outputs.write(Constants.DETAILS_BY_CITY_COUNTRY_TABLE_NAME, key, outputValue,
				Constants.DETAILS_BY_CITY_COUNTRY_TABLE_NAME + File.separator + "part");
		} 
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		outputs.close();
	}
}