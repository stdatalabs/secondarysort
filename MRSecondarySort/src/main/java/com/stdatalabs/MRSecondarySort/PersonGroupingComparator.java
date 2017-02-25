package com.stdatalabs.MRSecondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PersonGroupingComparator extends WritableComparator{
	
	protected PersonGroupingComparator() {
		super(Person.class,true);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public int compare(WritableComparable o1,WritableComparable o2){
		Person p1 = (Person) o1;
		Person p2 = (Person) o2;
		int cmp = p1.getLname().compareTo(p2.getLname());
		return cmp;
		
	}
	


}
