package fj.cmd;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.inject.Inject;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import fj.logic.EdgeLoader;
import fj.model.pojos.PsEdge;
import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.LoaderExecutor;
import io.fluo.api.config.FluoConfiguration;

public class Load {

  @Inject
  static FluoConfiguration fluoConfig;

  public static void main(String[] args) throws Exception {

    if(args.length < 3) {
      System.out.println("Usage : "+Load.class.getSimpleName()+" <remainder> <modulus> <file>{ file}");
      System.exit(1);
    }

    int remainder = Integer.parseInt(args[0]);
    int modulus = Integer.parseInt(args[1]);

    HashFunction hFunc = Hashing.murmur3_32();

    for(int i = 2; i < args.length; i++) {
      loadFile(modulus, remainder, args[i], hFunc);
    }
  }

  private static void loadFile(int modulus, int remainder, String filename, HashFunction hFunc)
      throws IOException {
    File file = new File(filename);
    String proj = file.getName().substring(0, file.getName().length() - 4);

    Iterator<PsEdge> edgeIter = Files.lines(file.toPath())
        .filter(l -> !l.startsWith("::::"))
        .filter(l -> Math.abs(hFunc.hashString(l).asInt()) % modulus == remainder)
        .map(l -> l.split(",", 2))
        .map(a -> new PsEdge(proj+"/"+a[0], a[1]))
        .sorted(PsEdge.SECOND_COMP)
        .iterator();

    try(FluoClient client = FluoFactory.newClient(fluoConfig); LoaderExecutor loader = client.newLoaderExecutor()){
      Set<PsEdge> edgeSet = new HashSet<>();

      int count = 0;
      while (edgeIter.hasNext()) {
        PsEdge psEdge = edgeIter.next();
        edgeSet.add(psEdge);
        count++;

        if(edgeSet.size() == 25) {
          loader.execute(new EdgeLoader(edgeSet));
          edgeSet = new HashSet<>();
        }
      }

      if(edgeSet.size() > 0) {
        loader.execute(new EdgeLoader(edgeSet));
      }

      System.out.printf("Loaded %d edges from %s\n", count, file.getName());
   }
  }
}
