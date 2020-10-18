package FuelCar;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import java.util.*;

public class FuelCarMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private final static IntWritable zero = new IntWritable(0);
	
	public void map(LongWritable key, Text value, OutputCollector <Text, IntWritable> output, Reporter reporter) throws IOException {

		String valueString = value.toString();
		String[] SingleCityData = valueString.split(";");
		if(SingleCityData[8].equals("gas")){
		output.collect(new Text(SingleCityData[1]), one);
		}
		else{
		output.collect(new Text(SingleCityData[1]),zero);
}
	}
}
