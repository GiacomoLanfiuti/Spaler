package spaler;

import java.io.Serializable;
import java.util.Objects;

public class ArcoPol extends Arco implements Serializable {
    private String polSrc;
    private String polDst;

    public ArcoPol(long src, long dst, String polSrc, String polDst) {
        super(src, dst);
        this.polSrc = polSrc;
        this.polDst = polDst;
    }

    public String getPolSrc() {
        return polSrc;
    }

    public void setPolSrc(String polSrc) {
        this.polSrc = polSrc;
    }

    public String getPolDst() {
        return polDst;
    }

    public void setPolDst(String polDst) {
        this.polDst = polDst;
    }

    public String toString() {
        return "(" + getSrc() + ", " + this.polSrc + ", " + getDst() + ", " + this.polDst + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Arco)) return false;
        ArcoPol arco = (ArcoPol) o;
        return getSrc() == arco.getSrc() && getDst() == arco.getDst() && getPolSrc().equals(arco.getPolSrc()) && getPolDst().equals(arco.getPolDst());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSrc(), getPolSrc(), getDst(), getPolDst());
    }

}