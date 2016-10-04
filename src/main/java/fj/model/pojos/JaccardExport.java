package fj.model.pojos;

public class JaccardExport {
  private final Double oldVal;
  private final Double newVal;

  public JaccardExport(Jaccard oldVal, Jaccard newVal) {
    if (oldVal != null) {
      this.oldVal = oldVal.toDouble();
    } else {
      this.oldVal = null;
    }

    if (newVal != null) {
      this.newVal = newVal.toDouble();
    } else {
      this.newVal = null;
    }
  }

  public Double getOldVal() {
    return oldVal;
  }

  public Double getNewVal() {
    return newVal;
  }
}
