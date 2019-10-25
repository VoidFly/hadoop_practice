
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomComparator extends WritableComparator{

    protected CustomComparator() {
        super(CombinationKey.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        CombinationKey ck1=(CombinationKey) a;
        CombinationKey ck2=(CombinationKey) b;
        int cp1 = ck1.getFirstKey().compareTo(ck2.getFirstKey());
        if(cp1!=0) {
            //结束排序
            return cp1;
        }else {
            return  ck1.getSecondKey()-ck2.getSecondKey();
        }
    }
}