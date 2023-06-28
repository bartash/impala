/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.service;

import static org.apache.impala.util.PatternMatcher.MATCHER_MATCH_ALL;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.impala.analysis.ColumnDef;
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.analysis.KuduPartitionParam;
import org.apache.impala.analysis.TableName;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.FeDataSource;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HdfsCachePool;
import org.apache.impala.catalog.IcebergContentFileStore;
import org.apache.impala.catalog.iceberg.IcebergHadoopCatalog;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TCompressionCodec;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TFunctionCategory;
import org.apache.impala.thrift.TGetPartitionStatsResponse;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TIcebergFileFormat;
import org.apache.impala.thrift.TIcebergPartitionStats;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.PatternMatcher;

public class StandaloneIcebergCatalog implements FeCatalog {

  private IcebergHadoopCatalog hadoopCatalog;

  public StandaloneIcebergCatalog() {
    this("file:///home/asherman/git/asf/Impala3/vw");
  }
  public StandaloneIcebergCatalog(String location) {
    String catalogLocation = "file:///home/asherman/git/asf/Impala3/vw";
    if (location != null) {
      catalogLocation = location;
    }
    try {
      URI uri = new URI(catalogLocation);
      File f = new File(uri.getPath());
      f.mkdirs();
      // Make the default db
      File def = new File(f.getAbsolutePath() + File.separator + Catalog.DEFAULT_DB);
      def.mkdirs();
    } catch (URISyntaxException e) {
      throw new RuntimeException("bad uri " + catalogLocation, e);
    }
    hadoopCatalog = new IcebergHadoopCatalog(catalogLocation);
  }

  @Override
  public List<? extends FeDb> getDbs(PatternMatcher matcher) {
    if (!matcher.equals(MATCHER_MATCH_ALL)) {
      // We can't cope with this yet
      throw new RuntimeException("unexpected pattern matcher " + matcher);
    }
    List<Namespace> namespaces = hadoopCatalog.getHadoopCatalog().listNamespaces();
    List<StandaloneIcebergDB> dbs = new ArrayList<>();
    for (Namespace namespace : namespaces) {
      dbs.add(new StandaloneIcebergDB(namespace.toString()));
    }
    return dbs;
  }



  @Override
  public List<String> getTableNames(String dbName, PatternMatcher matcher)
      throws DatabaseNotFoundException {
    Namespace ns = Namespace.of(dbName);
    List<TableIdentifier> tableIdentifiers = hadoopCatalog.getHadoopCatalog().listTables(ns);
    List<String> ret = new ArrayList<>();
    for (TableIdentifier tableIdentifier : tableIdentifiers) {
      ret.add(tableIdentifier.name());
    }
    return ret;
  }

  @Override
  public FeTable getTable(String dbName, String tableName) throws DatabaseNotFoundException {
    return getDb(dbName).getTable(tableName);
  }

  @Override
  public FeTable getTableNoThrow(String dbName, String tableName) {
    throw new IllegalStateException("getTableNoThrow not implemented in StandaloneIcebergCatalog");
  }

  @Override
  public FeTable getTableIfCached(String dbName, String tableName)
      throws DatabaseNotFoundException {
    throw new IllegalStateException("getTableIfCached not implemented in StandaloneIcebergCatalog");
  }

  @Override
  public FeTable getTableIfCachedNoThrow(String dbName, String tableName) {
    throw new IllegalStateException("getTableIfCachedNoThrow not implemented in StandaloneIcebergCatalog");
  }

  @Override
  public TCatalogObject getTCatalogObject(TCatalogObject objectDesc) throws CatalogException {
    throw new IllegalStateException("getTCatalogObject not implemented in StandaloneIcebergCatalog");
  }

  @Override
  public FeDb getDb(String db) {
    return new StandaloneIcebergDB(db);
  }

  @Override
  public FeFsPartition getHdfsPartition(String db, String tbl,
      List<TPartitionKeyValue> partitionSpec) throws CatalogException {
    throw new IllegalStateException("getHdfsPartition not implemented in StandaloneIcebergCatalog");
  }

  @Override
  public List<? extends FeDataSource> getDataSources(PatternMatcher createHivePatternMatcher) {
    throw new IllegalStateException("getDataSources not implemented in StandaloneIcebergCatalog");
  }

  @Override
  public FeDataSource getDataSource(String dataSourceName) {
    throw new IllegalStateException("getDataSource not implemented in StandaloneIcebergCatalog");
  }

  @Override
  public Function getFunction(Function desc, Function.CompareMode mode) {
    throw new IllegalStateException("getFunction not implemented in StandaloneIcebergCatalog");
  }

  @Override
  public HdfsCachePool getHdfsCachePool(String poolName) {
    throw new IllegalStateException("getHdfsCachePool not implemented in StandaloneIcebergCatalog");
  }

  @Override
  public void prioritizeLoad(Set<TableName> tableNames) throws InternalException {
    throw new IllegalStateException("prioritizeLoad not implemented in StandaloneIcebergCatalog");
  }

  @Override
  public TGetPartitionStatsResponse getPartitionStats(TableName table) throws InternalException {
    throw new IllegalStateException("getPartitionStats not implemented in StandaloneIcebergCatalog");
  }

  @Override
  public void waitForCatalogUpdate(long timeoutMs) {
    throw new IllegalStateException("waitForCatalogUpdate not implemented in StandaloneIcebergCatalog");
  }

  @Override
  public TUniqueId getCatalogServiceId() {
    throw new IllegalStateException("getCatalogServiceId not implemented in StandaloneIcebergCatalog");
  }

  @Override
  public AuthorizationPolicy getAuthPolicy() {
    throw new IllegalStateException("getAuthPolicy not implemented in StandaloneIcebergCatalog");
  }

  @Override
  public String getDefaultKuduMasterHosts() {
    throw new IllegalStateException("getDefaultKuduMasterHosts not implemented in StandaloneIcebergCatalog");
  }

  @Override
  public boolean isReady() {
    // For the sake or argument
    return true;
  }

  @Override
  public void setIsReady(boolean isReady) {
    throw new IllegalStateException("setIsReady not implemented in StandaloneIcebergCatalog");
  }

  class StandaloneIcebergDB implements FeDb {

    private final String name;

    StandaloneIcebergDB(String name) {
      this.name = name;
    }
    @Override
    public String getName() {
      return this.name;
    }

    @Override
    public Database getMetaStoreDb() {
      throw new IllegalStateException("getMetaStoreDb not implemented in StandaloneIcebergDB");
    }

    @Override
    public boolean containsTable(String tableName) {
      return getAllTableNames().contains(tableName);
    }

    @Override
    public FeTable getTable(String tbl) {
      if (containsTable(tbl)) {
        return new StandaloneIcebergTable(name);
      }
      return null;
    }

    @Override
    public FeTable getTableIfCached(String tbl) {
      throw new IllegalStateException("getTableIfCached not implemented in StandaloneIcebergDB");
    }

    @Override
    public List<String> getAllTableNames() {
      try {
        return StandaloneIcebergCatalog.this.getTableNames(getName(), MATCHER_MATCH_ALL);
      } catch (DatabaseNotFoundException e) {
        // Should not happen.
        throw new RuntimeException("Database " + getName() + " not found ", e);
      }
    }

    @Override
    public boolean isSystemDb() {
      return false;
    }

    @Override
    public Function getFunction(Function desc, Function.CompareMode mode) {
      throw new IllegalStateException("getFunction not implemented in StandaloneIcebergDB");
    }

    @Override
    public List<Function> getFunctions(String functionName) {
      throw new IllegalStateException("getFunctions not implemented in StandaloneIcebergDB");
    }

    @Override
    public List<Function> getFunctions(TFunctionCategory category, String function) {
      throw new IllegalStateException("getFunctions 2 not implemented in StandaloneIcebergDB");
    }

    @Override
    public List<Function> getFunctions(TFunctionCategory category, PatternMatcher patternMatcher) {
      throw new IllegalStateException("getFunctions 3 not implemented in StandaloneIcebergDB");
    }

    @Override
    public int numFunctions() {
      throw new IllegalStateException("numFunctions not implemented in StandaloneIcebergDB");
    }

    @Override
    public boolean containsFunction(String function) {
      throw new IllegalStateException("containsFunction not implemented in StandaloneIcebergDB");
    }

    @Override
    public TDatabase toThrift() {
      return new TDatabase(getName());
    }

    @Override
    public FeKuduTable createKuduCtasTarget(Table msTbl, List<ColumnDef> columnDefs, List<ColumnDef> primaryKeyColumnDefs, boolean isPrimaryKeyUnique, List<KuduPartitionParam> kuduPartitionParams) throws ImpalaRuntimeException {
      throw new IllegalStateException("createKuduCtasTarget not implemented in StandaloneIcebergDB");
    }

    @Override
    public FeFsTable createFsCtasTarget(Table msTbl) throws CatalogException {
      throw new IllegalStateException("createFsCtasTarget not implemented in StandaloneIcebergDB");
    }

    @Override
    public String getOwnerUser() {
      return null;
    }
  }

  class StandaloneIcebergTable implements FeIcebergTable {

    private String name;

    StandaloneIcebergTable(String name) {
      this.name = name;
    }

    @Override
    public IcebergContentFileStore getContentFileStore() {
      throw new IllegalStateException("getContentFileStore not implemented in StandaloneIcebergTable");
    }

    @Override
    public Map<String, TIcebergPartitionStats> getIcebergPartitionStats() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");

    }

    @Override
    public FeFsTable getFeFsTable() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public TIcebergCatalog getIcebergCatalog() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public org.apache.iceberg.Table getIcebergApiTable() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public String getIcebergCatalogLocation() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public TIcebergFileFormat getIcebergFileFormat() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public TCompressionCodec getIcebergParquetCompressionCodec() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public long getIcebergParquetRowGroupSize() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public long getIcebergParquetPlainPageSize() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public long getIcebergParquetDictPageSize() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public String getIcebergTableLocation() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public List<IcebergPartitionSpec> getPartitionSpecs() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public IcebergPartitionSpec getDefaultPartitionSpec() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public int getDefaultPartitionSpecId() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public THdfsTable transfromToTHdfsTable(boolean updatePartitionFlag) {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public boolean isLoaded() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public Table getMetaStoreTable() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public String getStorageHandlerClassName() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public TCatalogObjectType getCatalogObjectType() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public String getName() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public String getFullName() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public TableName getTableName() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public TImpalaTableType getTableType() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public String getTableComment() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public List<Column> getColumns() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public List<Column> getColumnsInHiveOrder() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public List<String> getColumnNames() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public List<Column> getClusteringColumns() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public List<Column> getNonClusteringColumns() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public int getNumClusteringCols() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public boolean isClusteringColumn(Column c) {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public Column getColumn(String name) {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public ArrayType getType() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public FeDb getDb() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public long getNumRows() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public TTableStats getTTableStats() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public TTableDescriptor toThriftDescriptor(int tableId, Set<Long> referencedPartitions) {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public long getWriteId() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public ValidWriteIdList getValidWriteIds() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }

    @Override
    public String getOwnerUser() {
      throw new IllegalStateException(" not implemented in StandaloneIcebergTable");
    }
  }
}
