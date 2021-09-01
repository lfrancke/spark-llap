package com.hortonworks.spark.sql.hive.llap;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.hortonworks.spark.sql.hive.llap.util.JobUtil;
import com.hortonworks.spark.sql.hive.llap.util.SchemaUtil;
import org.apache.hadoop.hive.llap.LlapBaseInputFormat;
import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.hive.llap.Schema;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Seq;


import static com.hortonworks.spark.sql.hive.llap.FilterPushdown.buildWhereClause;
import static com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil.projections;
import static com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil.randomAlias;
import static com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil.selectProjectAliasFilter;
import static com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil.selectStar;
import static com.hortonworks.spark.sql.hive.llap.util.JobUtil.replaceSparkHiveDriver;
import static scala.collection.JavaConversions.asScalaBuffer;

/**
 * 1. Spark pulls the unpruned schema -> readSchema()
 * 2. Spark pushes the pruned schema -> pruneColumns(..)
 * 3. Spark pushes the top-level filters -> pushFilters(..)
 * 4. Spark pulls the filters that are supported by datasource -> pushedFilters(..)
 * 5. Spark pulls factories, where factory/task are 1:1 -> createBatchDataReaderFactories(..)
 */
public class HiveWarehouseDataSourceReader
    implements SupportsPushDownRequiredColumns, SupportsScanColumnarBatch, SupportsPushDownFilters {

  //The pruned schema
  private StructType schema;

  //The original schema
  private StructType baseSchema;

  //Pushed down filters
  //
  //"It's possible that there is no filters in the query and pushFilters(Filter[])
  // is never called, empty array should be returned for this case."
  private Filter[] pushedFilters = new Filter[0];

  //SessionConfigSupport options
  private final Map<String, String> options;

  private static final Logger LOG = LoggerFactory.getLogger(HiveWarehouseDataSourceReader.class);

  public HiveWarehouseDataSourceReader(Map<String, String> options) {
    this.options = options;
  }

  //if(schema is empty) -> df.count()
  //else if(using table option) -> select *
  //else -> SELECT <COLUMNS> FROM (<RAW_SQL>) WHERE <FILTER_CLAUSE>
  private String getQueryString(String[] requiredColumns, Filter[] filters) {

    String selectCols = "count(*)";
    if (requiredColumns.length > 0) {
      selectCols = projections(requiredColumns);
    }
    String baseQuery;
    if (getQueryType() == StatementType.FULL_TABLE_SCAN) {
      baseQuery = selectStar(options.get("table"));
    } else {
      baseQuery = options.get("query");
    }

    Seq<Filter> filterSeq = asScalaBuffer(Arrays.asList(filters)).seq();
    String whereClause = buildWhereClause(baseSchema, filterSeq);
    return selectProjectAliasFilter(selectCols, baseQuery, randomAlias(), whereClause);
  }

  private StatementType getQueryType() {
    return StatementType.fromOptions(options);
  }

  protected StructType getTableSchema() throws Exception {
    replaceSparkHiveDriver();

    StatementType queryKey = getQueryType();
    String query;
    if (queryKey == StatementType.FULL_TABLE_SCAN) {
      String dbName = HWConf.DEFAULT_DB.getFromOptionsMap(options);
      SchemaUtil.TableRef tableRef = SchemaUtil.getDbTableNames(dbName, options.get("table"));
      query = selectStar(tableRef.databaseName, tableRef.tableName);
    } else {
      query = options.get("query");
    }
    LlapBaseInputFormat llapInputFormat = null;
    try {
      JobConf conf = JobUtil.createJobConf(options, query);
      llapInputFormat = new LlapBaseInputFormat(false, Long.MAX_VALUE);
      InputSplit[] splits = llapInputFormat.getSplits(conf, 0);
      LlapInputSplit schemaSplit = (LlapInputSplit) splits[0];
      Schema schema = schemaSplit.getSchema();
      return SchemaUtil.convertSchema(schema);
    } finally {
      if (llapInputFormat != null) {
        close();
      }
    }
  }

  @Override
  public StructType readSchema() {
    try {
      if (schema == null) {
        schema = getTableSchema();
        baseSchema = schema;
        LOG.info("!!!! Schema = {}", schema);
      }
      return schema;
    } catch (Exception e) {
      LOG.error("Unable to read table schema");
      throw new RuntimeException(e);
    }
  }

  //"returns unsupported filters."
  @Override
  public Filter[] pushFilters(Filter[] filters) {
    pushedFilters = Arrays.stream(filters).
        filter(filter -> FilterPushdown.buildFilterExpression(baseSchema, filter).isDefined()).
        toArray(Filter[]::new);

    return Arrays.stream(filters).
        filter(filter -> !FilterPushdown.buildFilterExpression(baseSchema, filter).isDefined()).
        toArray(Filter[]::new);
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    schema = requiredSchema;
  }

  private List<InputPartition<ColumnarBatch>> createBatchDataReaderFactories() {
    // Populate the schema
    readSchema();
    LOG.info("this.schema = {}", schema);
    LOG.info("this.pushedFilters = {}", Arrays.toString(pushedFilters));

    String queryString;
    try {
      queryString = getQueryString(SchemaUtil.columnNames(schema), pushedFilters);
    } catch (Exception e) {
      LOG.error("getQueryString", e);
      throw new RuntimeException(e);
    }

    boolean countStar = schema.isEmpty();
    List<InputPartition<ColumnarBatch>> factories = new ArrayList<>();
    if (countStar) {
      LOG.info("Executing count with query: {}", queryString);
      try {
        factories.addAll(getCountStarFactories(queryString));
      } catch (Exception e) {
        LOG.error("getCountStarFactories", e);
        throw new RuntimeException(e);
      }
    } else {
      try {
        factories.addAll(getSplitsFactories(queryString));
      } catch (Exception e) {
        LOG.error("getSplitsFactories", e);
        throw new RuntimeException(e);
      }
    }
    return factories;
  }

  @Override
  public List<InputPartition<ColumnarBatch>> planBatchInputPartitions() {
    return createBatchDataReaderFactories();
  }

  protected List<InputPartition<ColumnarBatch>> getSplitsFactories(String query) {
    List<InputPartition<ColumnarBatch>> tasks = new ArrayList<>();
    try {
      JobConf jobConf = JobUtil.createJobConf(options, query);
      LlapBaseInputFormat llapInputFormat = new LlapBaseInputFormat(false, Long.MAX_VALUE);
      //numSplits arg not currently supported, use 1 as dummy arg
      InputSplit[] splits = llapInputFormat.getSplits(jobConf, 1);
      for (InputSplit split : splits) {
        tasks.add(getDataReaderFactory(split, jobConf, getArrowAllocatorMax()));
      }
    } catch (IOException e) {
      LOG.error("Unable to submit query to HS2");
      throw new RuntimeException(e);
    }
    return tasks;
  }

  protected InputPartition<ColumnarBatch> getDataReaderFactory(InputSplit split, JobConf jobConf, long arrowAllocatorMax) {
    return new HiveWarehouseDataReaderFactory(split, jobConf, arrowAllocatorMax);
  }

  private List<InputPartition<ColumnarBatch>> getCountStarFactories(String query) {
    List<InputPartition<ColumnarBatch>> tasks = new ArrayList<>(100);
    long count = getCount(query);
    String numTasksString = HWConf.COUNT_TASKS.getFromOptionsMap(options);
    int numTasks = Integer.parseInt(numTasksString);
    long numPerTask = count / (numTasks - 1);
    for (int i = 0; i < (numTasks - 1); i++) {
      tasks.add(new CountDataReaderFactory(numPerTask));
    }
    long numLastTask = count % (numTasks - 1);
    tasks.add(new CountDataReaderFactory(numLastTask));
    return tasks;
  }

  protected long getCount(String query) {
    try (Connection conn = getConnection()) {
      DriverResultSet rs = DefaultJDBCWrapper.executeStmt(conn, HWConf.DEFAULT_DB.getFromOptionsMap(options), query,
          Long.parseLong(HWConf.MAX_EXEC_RESULTS.getFromOptionsMap(options)));
      return rs.getData().get(0).getLong(0);
    } catch (SQLException e) {
      LOG.error("Failed to connect to HS2", e);
      throw new RuntimeException(e);
    }
  }

  private Connection getConnection() {
    String url = HWConf.RESOLVED_HS2_URL.getFromOptionsMap(options);
    String user = HWConf.USER.getFromOptionsMap(options);
    String dbcp2Configs = HWConf.DBCP2_CONF.getFromOptionsMap(options);
    return DefaultJDBCWrapper.getConnector(Option.empty(), url, user, dbcp2Configs);
  }

  private long getArrowAllocatorMax() {
    String arrowAllocatorMaxString = HWConf.ARROW_ALLOCATOR_MAX.getFromOptionsMap(options);
    long arrowAllocatorMax = (Long) HWConf.ARROW_ALLOCATOR_MAX.defaultValue;
    if (arrowAllocatorMaxString != null) {
      arrowAllocatorMax = Long.parseLong(arrowAllocatorMaxString);
    }
    LOG.debug("Ceiling for Arrow direct buffers {}", arrowAllocatorMax);
    return arrowAllocatorMax;
  }

  public void close() {
    LOG.info("Closing resources for handleid: {}", options.get("handleid"));
    try {
      LlapBaseInputFormat.close(options.get("handleid"));
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

}
