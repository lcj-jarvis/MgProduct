package fai.MgProductStoreSvr.domain.comm;

import java.util.Objects;

/**
 * 实现排序接口，先比较 unionPriId 再比较 skuId
 */
public class SkuStoreKey implements Comparable<SkuStoreKey>  {
    public int unionPriId;
    public long skuId;

    public SkuStoreKey(int unionPriId, long skuId) {
        this.unionPriId = unionPriId;
        this.skuId = skuId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SkuStoreKey storeKey = (SkuStoreKey) o;
        return unionPriId == storeKey.unionPriId &&
                skuId == storeKey.skuId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(unionPriId, skuId);
    }

    @Override
    public String toString() {
        return "SkuStoreKey{" +
                "unionPriId=" + unionPriId +
                ", skuId=" + skuId +
                '}';
    }

    @Override
    public int compareTo(SkuStoreKey other) {
        int compareResult = Integer.compare(this.unionPriId, other.unionPriId);
        if(compareResult == 0){
            compareResult = Long.compare(this.skuId, other.skuId);
        }
        return compareResult;
    }
}