package spaler;

import java.io.Serializable;
import java.util.Objects;

public class Arco implements Serializable {
    private long src;
    private long dst;

    public Arco(long src, long dst) {
        this.src = src;
        this.dst = dst;
    }

    public long getSrc() {
        return src;
    }

    public void setSrc(long src) {
        this.src = src;
    }

    public long getDst() {
        return dst;
    }

    public void setDst(long dst) {
        this.dst = dst;
    }

    public String toString() {
        return "(" + this.src + ", " + this.dst + ")";
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Arco)) return false;
        Arco arco = (Arco) o;
        return getSrc() == arco.getSrc() && getDst() == arco.getDst();
    }

    public int hashCode() {
        return Objects.hash(getSrc(), getDst());
    }

}