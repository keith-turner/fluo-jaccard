package fj.model.pojos;

import java.util.Comparator;

public class PsEdge extends Edge<PNodeId, SNodeId> {
  public static final Comparator<PsEdge> SECOND_COMP = new Comparator<PsEdge>() {

    @Override
    public int compare(PsEdge o1, PsEdge o2) {
      int cmp = o1.getNode2().compareTo(o2.getNode2());
      if (cmp == 0) {
        cmp = o1.getNode1().compareTo(o1.getNode1());
      }
      return cmp;
    }
  };

  public PsEdge(PNodeId pn1, SNodeId pn2) {
    super(pn1, pn2);
  }

  public PsEdge(String pn1, String pn2) {
    super(new PNodeId(pn1), new SNodeId(pn2));
  }
}
