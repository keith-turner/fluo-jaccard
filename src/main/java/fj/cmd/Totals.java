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
import org.apache.commons.codec.binary.Hex;

public class Totals {

  static final ReverseLexicoder<Double> rl = new ReverseLexicoder<>(new DoubleLexicoder());

  @Inject
  static FluoConfiguration fluoConfig;

  public static void main(String[] args) throws Exception {
    Connector conn = CreateExport.getConnector(fluoConfig);

    Scanner scanner = conn.createScanner(GenConfig.EXPORT_TABLE_NAME, Authorizations.EMPTY);

    scanner.setRange(Range.prefix("t:"));

    for (Entry<Key, Value> entry : scanner) {
      double jaccard = rl
          .decode(Hex.decodeHex(entry.getKey().getRowData().toString().substring(2).toCharArray()));
      System.out.printf("%.3f %s %s\n", jaccard, entry.getKey().getColumnFamilyData(),
          entry.getKey().getColumnQualifierData());
    }
  }
}
