package com.phanindra.mostpopular;

import java.io.IOException;

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
		String val = value.toString();
		String request = extractRequest(val);
		String requestNoDomain = removeDomain(request);
		context.write(new Text(requestNoDomain), new LongWritable(1L));
	}
	
	private String extractRequest(String line) {
		int beginQuoteIndex = line.indexOf("\"");
		int closeQuoteIndex = line.lastIndexOf("\"");
		String requestLine = line.substring(beginQuoteIndex,
				closeQuoteIndex + 1);
		return requestLine;
	}
	
	private String removeDomain(String url) {
		String [] c =url.split("http://");
		if(c.length > 1 && c[1].lastIndexOf("?")!=-1) {
			return c[1].substring(c[1].indexOf("/"), c[1].lastIndexOf("?"));
		} else if(c.length > 1 && c[1].lastIndexOf("?")==-1) {
			return c[1].substring(c[1].indexOf("/"));
		}
		else {
			return url;
		}
	}

}
