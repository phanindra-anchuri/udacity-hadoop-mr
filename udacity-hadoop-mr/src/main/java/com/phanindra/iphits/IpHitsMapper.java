package com.phanindra.iphits;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("unused")
public class IpHitsMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String ip = context.getConfiguration().get("filter-ip");
		String line = value.toString();
		String[] lineComponents = line.split("\\s+");
		if(lineComponents != null && lineComponents.length > 0) {
			if(ip.equals(lineComponents[0])) {
				context.write(new Text(lineComponents[0]), new LongWritable(1L));
			}
		}
	}

}
