package com.atguigu.homework6.task3;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Amount implements Writable, WritableComparable<Amount> {
    private String user_id;
    private long amount;

    @Override
    public int compareTo(Amount o) {
        return -Long.compare(amount, o.amount);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(user_id);
        dataOutput.writeLong(amount);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        user_id = dataInput.readUTF();
        amount = dataInput.readLong();
    }

    @Override
    public String toString() {
        return user_id + '\t' + amount;
    }
}
