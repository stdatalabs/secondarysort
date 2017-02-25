package com.stdatalabs.MRSecondarySort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PersonPartitioner extends Partitioner<Person, Text>{

	@Override
	public int getPartition(Person key, Text value, int nuOfReducers) {
		// TODO Auto-generated method stub
		return (key.getLname().hashCode() % nuOfReducers);
	}

	

}
