package com.step7;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Key implements WritableComparable<Key>
{
    private Double calculation;
    private int decade;
    public Key() { calculation = 0.0; decade = 0;}
    public Key(double calculation, int decade) { this.calculation = calculation; this.decade = decade; }
    public double getCalculation() { return calculation; }
    public int getDecade() { return decade; }

    public void readFields(DataInput in) throws IOException
    {
        calculation = in.readDouble();
        decade = in.readInt();
    }

    public void write(DataOutput out) throws IOException
    {
        out.writeDouble(calculation);
        out.writeInt(decade);
    }

    public int compareTo(Key other)
    {
        int cmp = decade - other.decade;
        if (cmp!=0) {return cmp;}

        return -calculation.compareTo(other.calculation);
    }
}
