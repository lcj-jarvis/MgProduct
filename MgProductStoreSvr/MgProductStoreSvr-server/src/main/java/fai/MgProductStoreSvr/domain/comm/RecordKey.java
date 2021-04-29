package fai.MgProductStoreSvr.domain.comm;

import java.util.Objects;

public class RecordKey implements Comparable<RecordKey>{
    public long skuId;
    public int itemId;

    public RecordKey(long skuId, int itemId) {
        this.skuId = skuId;
        this.itemId = itemId;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecordKey recordKey = (RecordKey) o;
        return skuId == recordKey.skuId &&
                itemId == recordKey.itemId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(skuId, itemId);
    }

    @Override
    public String toString() {
        return "RecordKey{" +
                "skuId=" + skuId +
                ", itemId=" + itemId +
                '}';
    }

    @Override
    public int compareTo(RecordKey o) {
        int compareResult = Long.compare(this.skuId, o.skuId);
        if(compareResult != 0){
           return compareResult;
        }
        compareResult = Integer.compare(this.itemId, o.itemId);
        return compareResult;
    }
}
