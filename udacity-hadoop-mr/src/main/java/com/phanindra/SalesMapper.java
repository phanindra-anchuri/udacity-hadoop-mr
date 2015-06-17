package com.phanindra;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesMapper extends Mapper<Text, Text, Text, DoubleWritable> {

	@Override
	protected void map(Text key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineContents = line.split("\t");
		String category = lineContents[3];
		double amount = Float.parseFloat(lineContents[4]);
		context.write(category, new DoubleWritable(amount));
	}
	
	
}
