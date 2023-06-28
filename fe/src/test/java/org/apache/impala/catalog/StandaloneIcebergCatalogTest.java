package org.apache.impala.catalog;

import static org.apache.impala.util.PatternMatcher.MATCHER_MATCH_ALL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.impala.service.StandaloneIcebergCatalog;
import org.apache.impala.util.PatternMatcher;
import org.junit.Test;

import java.util.List;

public class StandaloneIcebergCatalogTest {

  @Test
  public void testDbs() {
    StandaloneIcebergCatalog catalog =
        new StandaloneIcebergCatalog("file:///home/asherman/git/asf/Impala3/empty");
    List<? extends FeDb> dbs = catalog.getDbs(MATCHER_MATCH_ALL);
    assertFalse(dbs.isEmpty());
    assertEquals(1, dbs.size());
    FeDb defaultDb = dbs.get(0);
    checkDefault(defaultDb);
    List<String> allTableNames = defaultDb.getAllTableNames();
    assertTrue(allTableNames.isEmpty());

    FeDb defaultDb2 = catalog.getDb(Catalog.DEFAULT_DB);
    checkDefault(defaultDb2);

    assertNull(defaultDb.getTable("foo"));

  }

  // Validation for default deb
  private static void checkDefault(FeDb db) {
    assertEquals(Catalog.DEFAULT_DB, db.getName());
    assertFalse(db.isSystemDb());
  }


}
