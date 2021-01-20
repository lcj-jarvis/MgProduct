package fai.MgProductStoreSvr.domain.comm;

import java.util.Objects;

public class SkuStoreKey {
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
}