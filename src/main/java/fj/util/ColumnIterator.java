package fj.util;

import java.util.Iterator;
import java.util.Map.Entry;

import io.fluo.api.client.SnapshotBase;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.Span;
import io.fluo.api.iterator.RowIterator;

public class ColumnIterator implements Iterator<Entry<Column, Bytes>> {

  private io.fluo.api.iterator.ColumnIterator colIter;

  public ColumnIterator(SnapshotBase tx, String row) {
    ScannerConfiguration config = new ScannerConfiguration().setSpan(Span.exact(row));
    RowIterator rowIter = tx.get(config);
    if (rowIter.hasNext()) {
      colIter = rowIter.next().getValue();
    }
  }

  @Override
  public boolean hasNext() {
    return colIter != null && colIter.hasNext();
  }

  @Override
  public Entry<Column, Bytes> next() {
    return colIter.next();
  }
}
