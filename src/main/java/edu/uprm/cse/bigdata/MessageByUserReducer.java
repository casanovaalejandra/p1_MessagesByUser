package edu.uprm.cse.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MessageByUserReducer extends Reducer<Text,Text,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // setup a counter
        String message = null;
        // iterator over list of 1s, to count them (no size() or length() method available)
        for (Text value : values ){
            message+=" "+ value.toString();

        }

        // DEBUG
        context.write(key, new Text(message));
    }
}
