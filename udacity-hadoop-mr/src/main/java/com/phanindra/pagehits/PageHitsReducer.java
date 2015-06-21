package com.phanindra.pagehits;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

@SuppressWarnings("unused")
public class PageHitsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("In reducer "+ key);
		Long hits = 0L;
		for(LongWritable value: values) {
			hits += value.get();
		}
		context.write(key, new LongWritable(hits));
	}
	
}
