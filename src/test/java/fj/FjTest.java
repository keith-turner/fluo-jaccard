package fj;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import fj.logic.EdgeLoader;
import fj.logic.PNodeObserver;
import fj.logic.SNodeObserver;
import fj.model.persistence.PNodePersistence;
import fj.model.pojos.Jaccard;
import fj.model.pojos.PNodeId;
import fj.model.pojos.PNodeInfo;
import fj.model.pojos.PsEdge;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.LoaderExecutor;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.ObserverConfiguration;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

public class FjTest {

  static Jaccard getJaccard(Snapshot snap, String pn1, String pn2) {
    PNodeInfo p1p2Info = PNodePersistence.getState(snap, new PNodeId(pn1)).getHighNeighbors().get(new PNodeId(pn2));
    return p1p2Info.getExportedJaccard();
  }

  static void addEdges(MiniFluo mini, FluoClient client, PsEdge ... edges) {
    Set<PsEdge> edges1 = new HashSet<PsEdge>();
    edges1.addAll(Arrays.asList(edges));
    try (LoaderExecutor loader = client.newLoaderExecutor()) {
      loader.execute(new EdgeLoader(edges1));
    }
    mini.waitForObservers();
  }

  @Test
  public void testBasic() throws Exception {
    FileUtils.deleteQuietly(new File("target/mini"));

    FluoConfiguration fluoConfig = new FluoConfiguration();
    fluoConfig.addObserver(new ObserverConfiguration(PNodeObserver.class.getName()));
    fluoConfig.addObserver(new ObserverConfiguration(SNodeObserver.class.getName()));

    fluoConfig.setMiniDataDir("target/mini");
    fluoConfig.setApplicationName("fj");

    try (MiniFluo mini = FluoFactory.newMiniFluo(fluoConfig);
        FluoClient client = FluoFactory.newClient(mini.getClientConfiguration())) {

      addEdges(mini, client, new PsEdge("p1","s1"), new PsEdge("p2","s1"), new PsEdge("p3","s1"));
      try(Snapshot snap = client.newSnapshot()){
        Assert.assertEquals(new Jaccard(1, 1), getJaccard(snap, "p1", "p2"));
        Assert.assertEquals(new Jaccard(1, 1), getJaccard(snap, "p1", "p3"));
        Assert.assertEquals(new Jaccard(1, 1), getJaccard(snap, "p2", "p3"));
      }

      addEdges(mini, client,new PsEdge("p2","s2"));
      try(Snapshot snap = client.newSnapshot()){
        Assert.assertEquals(new Jaccard(1, 2), getJaccard(snap, "p1", "p2"));
        Assert.assertEquals(new Jaccard(1, 1), getJaccard(snap, "p1", "p3"));
        Assert.assertEquals(new Jaccard(1, 2), getJaccard(snap, "p2", "p3"));
      }

      addEdges(mini, client,new PsEdge("p2","s3"),new PsEdge("p1","s4"),new PsEdge("p1","s5"), new PsEdge("p3","s6"));
      try(Snapshot snap = client.newSnapshot()){
        Assert.assertEquals(new Jaccard(1, 5), getJaccard(snap, "p1", "p2"));
        Assert.assertEquals(new Jaccard(1, 4), getJaccard(snap, "p1", "p3"));
        Assert.assertEquals(new Jaccard(1, 4), getJaccard(snap, "p2", "p3"));
      }

      addEdges(mini, client,new PsEdge("p1","s3"));
      try(Snapshot snap = client.newSnapshot()){
        Assert.assertEquals(new Jaccard(2, 5), getJaccard(snap, "p1", "p2"));
        Assert.assertEquals(new Jaccard(1, 5), getJaccard(snap, "p1", "p3"));
        Assert.assertEquals(new Jaccard(1, 4), getJaccard(snap, "p2", "p3"));
      }

      //re-add existing edges...
      addEdges(mini, client, new PsEdge("p1","s1"), new PsEdge("p2","s1"), new PsEdge("p3","s1"));
      try(Snapshot snap = client.newSnapshot()){
        Assert.assertEquals(new Jaccard(2, 5), getJaccard(snap, "p1", "p2"));
        Assert.assertEquals(new Jaccard(1, 5), getJaccard(snap, "p1", "p3"));
        Assert.assertEquals(new Jaccard(1, 4), getJaccard(snap, "p2", "p3"));
      }
    }
  }
}
