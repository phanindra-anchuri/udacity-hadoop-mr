package com.phanindra;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

@SuppressWarnings("unused")
public class SalesReducer extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		Double sum = 0.0;
		for(DoubleWritable value: values) {
			sum += value.get();
		}
		context.write(key, new DoubleWritable(sum));
	}
}
