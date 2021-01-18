package fai.MgProductInfSvr.domain.comm;

import java.util.Objects;

public class BizPriKey {
    public int tid;
    public int siteId;
    public int lgId;
    public int keepPriId1;

    public BizPriKey(int tid, int siteId, int lgId, int keepPriId1) {
        this.tid = tid;
        this.siteId = siteId;
        this.lgId = lgId;
        this.keepPriId1 = keepPriId1;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BizPriKey bizPriKey = (BizPriKey) o;
        return tid == bizPriKey.tid &&
                siteId == bizPriKey.siteId &&
                lgId == bizPriKey.lgId &&
                keepPriId1 == bizPriKey.keepPriId1;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tid, siteId, lgId, keepPriId1);
    }

    @Override
    public String toString() {
        return "BizPriKey{" +
                "tid=" + tid +
                ", siteId=" + siteId +
                ", lgId=" + lgId +
                ", keepPriId1=" + keepPriId1 +
                '}';
    }
}
