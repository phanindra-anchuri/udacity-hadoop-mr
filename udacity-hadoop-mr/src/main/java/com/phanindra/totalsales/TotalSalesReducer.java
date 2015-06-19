package com.phanindra.totalsales;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalSalesReducer extends
		Reducer<Text, DoubleWritable, LongWritable, DoubleWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		Long numberOfSales = 0L;
		Double totalSalesAmount = 0.0;
		for(DoubleWritable value: values) {
			numberOfSales++;
			totalSalesAmount += value.get();
		}
		context.write(new LongWritable(numberOfSales), new DoubleWritable(totalSalesAmount));
	}
}
