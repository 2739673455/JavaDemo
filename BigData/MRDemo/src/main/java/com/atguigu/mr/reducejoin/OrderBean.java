package com.atguigu.mr.reducejoin;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderBean implements WritableComparable<OrderBean> {
    private String id;
    private String pid;
    private String pname;
    private long amount;


    @Override
    public int compareTo(OrderBean o) {
        int result = pid.compareTo(o.getPid());
        if (result == 0) {
            result = -pname.compareTo(o.getPname());
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeUTF(pname);
        out.writeLong(amount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readUTF();
        pid = in.readUTF();
        pname = in.readUTF();
        amount = in.readLong();
    }

    @Override
    public String toString() {
        return id + '\t' + pname + '\t' + amount;
    }
}
