package com.atguigu.homework5.task2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Amount implements Writable {
    private long totalAmount;
    private long avgAmount;
    private long maxAmount;
    private long minAmount;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(totalAmount);
        dataOutput.writeLong(avgAmount);
        dataOutput.writeLong(maxAmount);
        dataOutput.writeLong(minAmount);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        totalAmount = dataInput.readLong();
        avgAmount = dataInput.readLong();
        maxAmount = dataInput.readLong();
        minAmount = dataInput.readLong();
    }

    public String toString() {
        return totalAmount + "\t" + avgAmount + "\t" + maxAmount + "\t" + minAmount;
    }
}
