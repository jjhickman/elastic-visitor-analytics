package data.analytics.mapreduce;

import java.io.IOException;
import java.io.File;
import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.net.URL;

import data.analytics.constants.Constants;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;

public class VisitorMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text outputKey = new Text("");
	private Text outputValue = new Text();
	DatabaseReader reader;
	BufferedInputStream inputStream;
	FileOutputStream fileOS;

	@Override
	public void setup(Context context) throws IOException {
		try {
			inputStream = new BufferedInputStream(new URL("https://{bucket_name}.s3.amazonaws.com/resources/GeoLite2-City.mmdb").openStream());
			fileOS = new FileOutputStream("GeoLite2-City.mmdb");
			    byte data[] = new byte[1024];
			    int byteContent;
			    while ((byteContent = inputStream.read(data, 0, 1024)) != -1) {
			        fileOS.write(data, 0, byteContent);
			 }
			File database = new File("GeoLite2-City.mmdb");
			reader = new DatabaseReader.Builder(database).build();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}

			if (fileOS != null) {
				fileOS.close();
			}
		}
	}

	@Override
	public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {

		// record: Date, ipAddress

		if (record.toString().charAt(0) == '#') {
			return;
		}

		String[] rows = record.toString().split("\\t");

		/*
		 * Fields: date time x-edge-location sc-bytes c-ip cs-method cs(Host)
		 * cs-uri-stem sc-status cs(Referer) cs(User-Agent) cs-uri-query cs(Cookie)
		 * x-edge-result-type x-edge-request-id
		 */
		String date = rows[0];
		String xEdgeLocation = rows[2];
		String scBytes = rows[3];
		String cIP = rows[4];
		String csURIStem = rows[7];
		String scStatus = rows[8];
		String xEdgeResultType = rows[13];

		String city = "";
		String country = "";
		try {
			InetAddress address = InetAddress.getByName(cIP);
			CityResponse response = reader.city(address);
			city = response.getCity().toString();
			country = response.getCountry().toString();
		} catch (GeoIp2Exception e) {
			e.printStackTrace();
			context.getCounter(Constants.CUSTOM_COUNTERS_GROUP_NAME, Constants.IP_TO_CITY_COUNTRY_MISS_COUNTER)
					.increment(1);
		}

		String filename = csURIStem.substring(csURIStem.lastIndexOf('/') + 1);

		// key = date, filename, scStatus, city, country, xEdgeLocation
		outputKey.set(date + "," + filename + "," + scStatus + "," + city + "," + country + "," + xEdgeLocation);
		// value = requestCount, hitCount, missCount, errorCount, bytes
		outputValue.set("1," + (xEdgeResultType.toString().equals("Hit") ? "1," : "0,")
				+ (xEdgeResultType.toString().equals("Miss") ? "1," : "0,")
				+ ((!xEdgeResultType.toString().equals("Miss") && !xEdgeResultType.toString().equals("Hit")) ? "1,"
						: "0,")
				+ scBytes);
		context.write(outputKey, outputValue);
		context.getCounter(Constants.CUSTOM_COUNTERS_GROUP_NAME, Constants.DETAILS_BY_CITY_COUNTRY_COUNTER_NAME)
				.increment(1);

	}
}