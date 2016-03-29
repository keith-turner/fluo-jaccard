package fj.model.pojos;

public class SNodeId implements Comparable<SNodeId> {

  private final String id;

  public SNodeId(String id) {
    this.id = id;
  }

  @Override
  public int compareTo(SNodeId o) {
    return id.compareTo(o.id);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SNodeId) {
      SNodeId osnid = (SNodeId) obj;
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
