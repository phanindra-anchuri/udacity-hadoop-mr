package com.phanindra.mostpopular;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("unused")
public class MostPopularMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String url = getUrl(line);
		String fileName = removeDomain(url);
		context.write(new Text(fileName), new LongWritable(1L));
	}
	
	private String getUrl(String line) {
		String[] lineParts = line.split("\\s+");
		return lineParts[6];
	}

	private String removeDomain(String url) {
		String result = "";
		URL u;
		try {
			u = new URL(url);
			result = u.getFile();
		} catch (MalformedURLException e) {
			if(e.getMessage().contains("no protocol")) {
				result = url;
			}
		}
		return result;
	}
	
	public static void main(String[] args) {
		String line = "10.64.224.191 - - [03/Dec/2011:13:25:20 -0800] \"GET http://www.the-associates.co.uk/search/index.php?query=Darker+than+black&p=1 HTTP/1.1\" 200 2845";
		MostPopularMapper mapper = new MostPopularMapper();
		System.out.println(mapper.removeDomain(mapper.getUrl(line)));
	}
}
