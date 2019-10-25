
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class CombinationKey implements WritableComparable<CombinationKey>{

    private String firstKey;
    private Integer secondKey;
    public String getFirstKey() {
        return firstKey;
    }

    public void setFirstKey(String firstKey) {
        this.firstKey = firstKey;
    }

    public Integer getSecondKey() {
        return secondKey;
    }

    public void setSecondKey(Integer secondKey) {
        this.secondKey = secondKey;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.firstKey);
        out.writeInt(this.secondKey);
    }

    public void readFields(DataInput in) throws IOException {
        this.firstKey=in.readUTF();
        this.secondKey=in.readInt();
    }

    public int compareTo(CombinationKey o) {
        return this.firstKey.compareTo(o.getFirstKey());
    }
}