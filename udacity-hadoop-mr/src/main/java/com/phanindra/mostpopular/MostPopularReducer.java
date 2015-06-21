package com.phanindra.mostpopular;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

@SuppressWarnings("unused")
public class MostPopularReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	
	private Map<String, Long> pageCount = new HashMap<String, Long>() ;
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		Long hits = 0L;
		for(LongWritable value: values) {
			hits += value.get();
		}
		pageCount.put(key.toString(), hits);
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		String key = Collections.max(pageCount.entrySet(),
				new Comparator<Entry<String, Long>>() {

					public int compare(Entry<String, Long> o1,
							Entry<String, Long> o2) {
						return o1.getValue() > o2.getValue() ? 1 : -1;
					}
				}).getKey();
		context.write(new Text(key), new LongWritable(pageCount.get(key)));
	}
	
	
	
}
