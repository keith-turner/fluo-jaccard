package fj.cmd;

import java.util.Map.Entry;

import javax.inject.Inject;

import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
import org.apache.accumulo.core.client.lexicoder.ReverseLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

public class Query {

  static final ReverseLexicoder<Double> rl = new ReverseLexicoder<>(new DoubleLexicoder());

  @Inject
  static FluoConfiguration fluoConfig;

  public static void main(String[] args) throws Exception {
    Connector conn = CreateExport.getConnector(fluoConfig);

    Scanner scanner = conn.createScanner(GenConfig.EXPORT_TABLE_NAME, Authorizations.EMPTY);

    scanner.setRange(new Range(args[0]));

    for (Entry<Key, Value> entry : scanner) {
      double jaccard = rl.decode(entry.getKey().getColumnFamilyData().toArray());
      System.out.printf("%.3f %s\n" , jaccard, entry.getKey().getColumnQualifierData());
    }
  }
}
