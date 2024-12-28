package com.atguigu.homework5.task3;

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
public class Info implements Writable {
    private String fileName;
    private long count;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(fileName);
        dataOutput.writeLong(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        fileName = dataInput.readUTF();
        count = dataInput.readLong();
    }
}
