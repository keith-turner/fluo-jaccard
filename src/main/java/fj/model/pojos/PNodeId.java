package fj.model.pojos;

public class PNodeId implements Comparable<PNodeId> {

  private String id;

  public PNodeId() {}

  public PNodeId(String id) {
    this.id = id;
  }

  @Override
  public int compareTo(PNodeId o) {
    return id.compareTo(o.id);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PNodeId) {
      PNodeId osnid = (PNodeId) obj;
      return id.equals(osnid.id);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public String toString() {
    return id;
  }

  public String getId() {
    return id;
  }
}
