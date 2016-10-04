package fj.model.pojos;

import com.google.common.base.Preconditions;

public class PNodeInfo {

  public static final PNodeInfo ZERO = new PNodeInfo();

  private int intersection;
  private int degree;
  private Jaccard exportedJaccard;

  public PNodeInfo(int intersection, int degree, Jaccard exportedJaccard) {
    Preconditions.checkArgument(intersection >= 0);
    Preconditions.checkArgument(degree >= 0);
    this.intersection = intersection;
    this.degree = degree;
    this.exportedJaccard = exportedJaccard;
  }

  public PNodeInfo() {
    intersection = 0;
    degree = 0;
    exportedJaccard = null;
  }

  public PNodeInfo(PNodeInfo other) {
    this.intersection = other.intersection;
    this.degree = other.degree;
    this.exportedJaccard = other.exportedJaccard;
  }

  public int getIntersection() {
    return intersection;
  }

  public void setIntersection(int intersection) {
    Preconditions.checkArgument(intersection >= 0);
    this.intersection = intersection;
  }

  public int getDegree() {
    return degree;
  }

  public void setDegree(int degree) {
    Preconditions.checkArgument(degree >= 0);
    this.degree = degree;
  }

  public Jaccard getExportedJaccard() {
    return exportedJaccard;
  }

  public void setExportedJaccard(Jaccard exportedJaccard) {
    this.exportedJaccard = exportedJaccard;
  }

  public void incrementIntersection(int update) {
    Preconditions.checkArgument(update >= 0);
    intersection += update;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof PNodeInfo) {
      PNodeInfo opi = (PNodeInfo) o;
      if (degree != opi.degree || intersection != opi.intersection) {
        return false;
      }

      if (exportedJaccard == null ^ opi.exportedJaccard == null) {
        // one is null and the other is not
        return false;
      }

      if (exportedJaccard == null) {
        return true;
      }

      return exportedJaccard.equals(opi.exportedJaccard);
    }

    return false;
  }

  @Override
  public String toString() {
    return intersection + " " + degree + " " + exportedJaccard;
  }
}
