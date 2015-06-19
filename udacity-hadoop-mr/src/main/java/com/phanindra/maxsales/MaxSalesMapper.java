package com.phanindra.maxsales;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("unused")
public class MaxSalesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineContents = line.split("\t");
		String store = lineContents[2];
		double amount = Double.parseDouble(lineContents[4]);
		context.write(new Text(store), new DoubleWritable(amount));
	}
	
	
}
