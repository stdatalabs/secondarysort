package com.stdatalabs.MRSecondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Person implements WritableComparable<Person>{

	private Text lname,fname;
	public Person() {
		lname= new Text();
		fname= new Text();
	}
	public Person(String lastName,String firstName) {
		this(new Text(lastName), new Text(firstName));
	}
	public Person(Text lastName, Text FirstName) {
		this.lname=lastName;
		this.fname=FirstName;
	}
	
	public Text getLname() {
		return lname;
	}
	public void setLname(Text lname) {
		this.lname = lname;
	}
	public Text getFname() {
		return fname;
	}
	public void setFname(Text fname) {
		this.fname = fname;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		lname.readFields(in);
		fname.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		lname.write(out);
		fname.write(out);
	}

	@Override
	public int compareTo(Person o) {
		// TODO Auto-generated method stub
		int cmp = lname.compareTo(o.lname);
		if (cmp==0){
			return fname.compareTo(o.fname);
		}
		return cmp;
	}


}
