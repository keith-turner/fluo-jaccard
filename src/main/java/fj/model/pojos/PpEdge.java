package fj.model.pojos;

public class PpEdge extends Edge<PNodeId, PNodeId> {

  private static PNodeId low(PNodeId pn1, PNodeId pn2) {
    int cmp = pn1.compareTo(pn2);
    if (cmp < 0) {
      return pn1;
    }
    return pn2;
  }

  private static PNodeId high(PNodeId pn1, PNodeId pn2) {
    int cmp = pn1.compareTo(pn2);
    if (cmp < 0) {
      return pn2;
    }
    return pn1;
  }

  public PpEdge(PNodeId pn1, PNodeId pn2) {
    super(low(pn1, pn2), high(pn1, pn2));
  }

  public PpEdge() {
    super();
  }
}
