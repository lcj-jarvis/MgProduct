package fai.MgProductStoreSvr.domain.comm;

import java.util.Objects;

public class PdKey {
    public int unionPriId;
    public int pdId;
    public int rlPdId;
    public int sysType;

    public PdKey(int unionPriId, int pdId, int rlPdId, int sysType) {
        this.unionPriId = unionPriId;
        this.pdId = pdId;
        this.rlPdId = rlPdId;
        this.sysType = sysType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PdKey pdKey = (PdKey) o;
        return unionPriId == pdKey.unionPriId &&
                pdId == pdKey.pdId &&
                rlPdId == pdKey.rlPdId &&
                sysType == pdKey.sysType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(unionPriId, pdId, rlPdId, sysType);
    }

    @Override
    public String toString() {
        return "PdKey{" +
                "unionPriId=" + unionPriId +
                ", pdId=" + pdId +
                ", rlPdId=" + rlPdId +
                ", sysType=" + sysType +
                '}';
    }
}
