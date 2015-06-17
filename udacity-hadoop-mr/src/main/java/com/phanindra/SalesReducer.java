package com.phanindra;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	protected void reduce(Text arg0, Iterable<DoubleWritable> arg1,
			org.apache.hadoop.mapreduce.Reducer.Context arg2)
			throws IOException, InterruptedException {
		Double sum = 0.0;
		for(DoubleWritable d: arg1) {
			sum += d.get();
		}
		arg2.write(arg0, sum);
	}
}
