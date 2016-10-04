package fj.logic;

import java.util.HashSet;
import java.util.Set;

import fj.model.pojos.PNodeId;
import fj.model.pojos.PpEdge;
import org.junit.Assert;
import org.junit.Test;

public class SNodeObserverTest {

  Set<PNodeId> newPnidSet(String... ids) {
    HashSet<PNodeId> pnids = new HashSet<>();

    for (String id : ids) {
      pnids.add(new PNodeId(id));
    }

    return pnids;
  }

  @Test
  public void testCombinations() {

    HashSet<PpEdge> expected = new HashSet<>();
    for (int i = 1; i <= 5; i++) {
      for (int j = 1; j <= 5; j++) {
        if (i < j) {
          expected.add(new PpEdge(new PNodeId(i + ""), new PNodeId(j + "")));
        }
      }
    }

    Assert.assertEquals(10, expected.size());


    Assert.assertNotEquals(expected,
        SNodeObserver.newCombinations(newPnidSet(), newPnidSet("1", "2", "3", "4")));
    Assert.assertEquals(expected,
        SNodeObserver.newCombinations(newPnidSet(), newPnidSet("1", "2", "3", "4", "5")));


    HashSet<PpEdge> cumlative = new HashSet<>();
    cumlative.addAll(SNodeObserver.newCombinations(newPnidSet(), newPnidSet("1")));
    Assert.assertEquals(0, cumlative.size());
    cumlative.addAll(SNodeObserver.newCombinations(newPnidSet("1"), newPnidSet("2")));
    Assert.assertEquals(1, cumlative.size());
    cumlative.addAll(SNodeObserver.newCombinations(newPnidSet("1", "2"), newPnidSet("3")));
    Assert.assertEquals(3, cumlative.size());
    cumlative.addAll(SNodeObserver.newCombinations(newPnidSet("1", "2", "3"), newPnidSet("4")));
    Assert.assertEquals(6, cumlative.size());
    cumlative
        .addAll(SNodeObserver.newCombinations(newPnidSet("1", "2", "3", "4"), newPnidSet("5")));
    Assert.assertEquals(expected, cumlative);

    cumlative.clear();
    cumlative.addAll(SNodeObserver.newCombinations(newPnidSet(), newPnidSet("1", "2", "3")));
    Assert.assertEquals(3, cumlative.size());
    cumlative
        .addAll(SNodeObserver.newCombinations(newPnidSet("1", "2", "3"), newPnidSet("4", "5")));
    Assert.assertEquals(expected, cumlative);


  }
}
