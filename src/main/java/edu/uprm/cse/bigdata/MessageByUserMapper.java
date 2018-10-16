package edu.uprm.cse.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;

public class MessageByUserMapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        //Esta clase lo que deber hacer es leer cada tweet y unirlo con su usuario
        String tweet = value.toString();
        String text = null;
        Status status = null;
        try {
            status = TwitterObjectFactory.createStatus(tweet);
            text = status.getText();
            context.write(new Text(Long.toString(status.getUser().getId())),new Text(text));
        } catch (TwitterException e) {
            e.printStackTrace();
        }
    }
}
