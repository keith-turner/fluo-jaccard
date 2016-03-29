package fj.model.pojos;

import com.google.common.base.Preconditions;

public class Jaccard {

  private int intersection;
  private int union;

  public Jaccard(){}

  public Jaccard(int intersection, int degree1, int degree2) {
    Preconditions.checkArgument(intersection >= 0, "negative intersection %s", intersection);
    Preconditions.checkArgument(degree1 >= 0, "negative degree1 %s", degree1);
    Preconditions.checkArgument(degree2 >= 0, "negative degree2 %s", degree2);
    this.intersection = intersection;
    this.union = degree1 + degree2 - intersection;
  }

  public Jaccard(int intersection, int union) {
    this.intersection = intersection;
    this.union = union;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Jaccard) {
      Jaccard oj = (Jaccard) o;
      return intersection == oj.intersection && union == oj.union;
    }
    return false;
  }

  public double toDouble() {
    return intersection / (double) (union);
  }

  public boolean isValid() {
    return union > 0 && intersection > 0 && intersection <= union;
  }

  public int getIntersection() {
    return intersection;
  }

  public int getUnion() {
    return union;
  }

  @Override
  public String toString(){
    return intersection+" "+union;
  }
}
