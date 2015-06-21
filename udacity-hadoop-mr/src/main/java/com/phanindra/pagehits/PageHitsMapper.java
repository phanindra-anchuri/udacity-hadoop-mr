package com.phanindra.pagehits;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("unused")
public class PageHitsMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String val = value.toString();
		int beginQuoteIndex = val.indexOf("\"");
		int closeQuoteIndex = val.lastIndexOf("\"");
		if (beginQuoteIndex != -1 && closeQuoteIndex != -1) {
			String requestLine = val.substring(beginQuoteIndex,
					closeQuoteIndex + 1);
			String[] requestComponents = requestLine.split("\\s");
			String page = requestComponents[1];
			if (page != null) {
				context.write(new Text(page), new LongWritable(1L));
			}
		}
	}

}
