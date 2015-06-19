package com.phanindra.maxsales;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

@SuppressWarnings("unused")
public class MaxSalesReducer extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		Double max = Double.MIN_VALUE;
		for(DoubleWritable value: values) {
			if(value.get() > max) {
				max = value.get();
			}
		}
		context.write(key, new DoubleWritable(max));
	}
}
