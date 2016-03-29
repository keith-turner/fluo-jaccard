package fj.model.pojos;

import java.util.Objects;

import com.google.common.base.Preconditions;

public class Edge<NT1, NT2> {
  private NT1 pn1;
  private NT2 pn2;


  public Edge(NT1 pn1, NT2 pn2) {
    Objects.requireNonNull(pn1);
    Objects.requireNonNull(pn2);
    Preconditions.checkArgument(!pn1.equals(pn2), "Illegal self edge %s %s", pn1, pn2);

    this.pn1 = pn1;
    this.pn2 = pn2;
  }

  public Edge() {}

  public NT1 getNode1() {
    return pn1;
  }

  public NT2 getNode2() {
    return pn2;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Edge) {
      Edge<?, ?> op = (Edge<?, ?>) o;
      return pn1.equals(op.pn1) && pn2.equals(op.pn2);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return pn1.hashCode() + 31 * pn2.hashCode();
  }

  @Override
  public String toString() {
    return pn1.toString() + ":" + pn2.toString();

  }
}
