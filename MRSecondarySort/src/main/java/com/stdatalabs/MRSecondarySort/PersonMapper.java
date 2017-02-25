package com.stdatalabs.MRSecondarySort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PersonMapper extends Mapper<Text, Text, Person, Text>{

	Person p = new Person();
	public void map(Text lName,Text fName, Context context) throws IOException, InterruptedException{
		
		p.setFname(fName);
		p.setLname(lName);
		context.write(p, fName);		
	}

}
