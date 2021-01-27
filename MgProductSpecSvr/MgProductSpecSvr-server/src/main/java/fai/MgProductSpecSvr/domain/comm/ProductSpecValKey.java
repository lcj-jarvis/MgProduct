package fai.MgProductSpecSvr.domain.comm;

import java.util.Objects;

public class ProductSpecValKey {
    public int pdScId; // 规格id
    public int inPdScValListScStrId; // 规格值中的id

    public ProductSpecValKey(int pdScId, int inPdScValListScStrId) {
        this.pdScId = pdScId;
        this.inPdScValListScStrId = inPdScValListScStrId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductSpecValKey that = (ProductSpecValKey) o;
        return pdScId == that.pdScId &&
                inPdScValListScStrId == that.inPdScValListScStrId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pdScId, inPdScValListScStrId);
    }

    @Override
    public String toString() {
        return "SpecValKey{" +
                "pdScId=" + pdScId +
                ", inPdScValListScStrId=" + inPdScValListScStrId +
                '}';
    }
}
