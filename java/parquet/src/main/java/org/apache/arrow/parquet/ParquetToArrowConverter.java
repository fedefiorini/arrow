/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.parquet;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.parquet.utils.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.apache.parquet.arrow.schema.SchemaMapping;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/***
 * This class provides the necessary logic to convert a Parquet File into
 * an usable Arrow data structure (in particular, a VectorSchemaRoot) and
 * populate it using the Parquet column values.
 *
 * The final aim for this class and the utilities is to be pushed into
 * Arrow codebase, in order to provide full Java support for reading
 * Parquet files without having to deal with native (JNI) code calls.
 */
public class ParquetToArrowConverter {
  private Configuration configuration;
  private RootAllocator allocator;
  private MessageType parquetSchema;
  public Schema arrowSchema;
  private VectorSchemaRoot vectorSchemaRoot;

  private List<Double> time = new ArrayList<>();
  private Long t0, t1, t2, t3, t4, t5, t6, t7 = 0L;

  public ParquetToArrowConverter() {
    t0 = System.nanoTime();
    this.configuration = new Configuration();
    this.allocator = new RootAllocator(Integer.MAX_VALUE);
    t1 = System.nanoTime();
    time.add((t1 - t0) / 1e9d);
  }

  /***
   * Main logic of the ParquetToArrowConverter.
   * It takes a string-formatted path from which to read the Parquet input file, then proceeds
   * to retrieve its schema and convert it to an Arrow schema using
   * [[org.apache.parquet.arrow.schema]] operations available (SchemaConverter and SchemaMapping).
   *
   * Once the Arrow schema is created, it's then used to create an [[org.apache.arrow.vector.VectorSchemaRoot]]
   * which is then populated using the values read from the Parquet file, where each Parquet column
   * then corresponds to an Arrow column (or [[org.apache.arrow.vector.FieldVector]]).
   *
   * This method has no return value, it just creates the VectorSchemaRoot and its associated
   * vectors and populates them.
   * There's therefore a series of helper methods to retrieve specific vectors from the VectorSchemaRoot.
   *
   * So far, only INT32 (Int), INT64 (BigInt or Long) and BINARY (VarBinary, String/Array[Byte])
   * have been implemented. The rest of the primitive data types can be easily implemented in future.
   */
  public void process(String path) throws Exception {
    t2 = System.nanoTime();
    HadoopInputFile inputFile = HadoopInputFile.fromPath(new Path(path), configuration);
    ParquetFileReader reader = ParquetFileReader.open(inputFile);
    parquetSchema = reader.getFileMetaData().getSchema();
    t3 = System.nanoTime();
    time.add((t3 - t2) / 1e9d);

    t4 = System.nanoTime();
    SchemaConverter converter = new SchemaConverter();
    SchemaMapping mapping = converter.fromParquet(parquetSchema);
    arrowSchema = mapping.getArrowSchema();
    vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator);
    t5 = System.nanoTime();
    time.add((t5 - t4) / 1e9d);

    /* 26.08: The following part has been taken from A. Trivedi's implementation directly,
     * see @link{https://gist.github.com/animeshtrivedi/76de64f9dab1453958e1d4f8eca1605f}
     *
     * In case a better alternative arises, it will be included here. */
    t6 = System.nanoTime();
    List<ColumnDescriptor> colDesc = parquetSchema.getColumns();
    List<FieldVector> vectors = vectorSchemaRoot.getFieldVectors();
    int size = colDesc.size();
    PageReadStore pageReadStore = reader.readNextRowGroup();
    while (pageReadStore != null) {
      ColumnReadStoreImpl colReader =
          new ColumnReadStoreImpl(
              pageReadStore,
              new DumpGroupConverter(),
              parquetSchema,
              reader.getFileMetaData().getCreatedBy());

      int rows = (int) pageReadStore.getRowCount();
      int i = 0;
      while (i < size) {
        ColumnDescriptor col = colDesc.get(i);
        switch (col.getPrimitiveType().getPrimitiveTypeName()) {
          case INT32:
            writeIntColumn(
                colReader.getColumnReader(col), col.getMaxDefinitionLevel(), vectors.get(i), rows);
            break;
          case INT64:
            writeLongColumn(
                colReader.getColumnReader(col), col.getMaxDefinitionLevel(), vectors.get(i), rows);
            break;
          case BINARY:
            writeBinaryColumn(
                colReader.getColumnReader(col), col.getMaxDefinitionLevel(), vectors.get(i), rows);
            break;
          default:
            throw new Exception("Unsupported primitive type");
        }
        i++;
      }
      pageReadStore = reader.readNextRowGroup();
    }
    t7 = System.nanoTime();
    time.add((t7 - t6) / 1e9d);
  }

  public List<Double> getTime() {
    return time;
  }

  /******* Helper public methods to retrieve VectorSchemaRoot and individual vectors *******/
  public VectorSchemaRoot getVectorSchemaRoot() {
    return vectorSchemaRoot;
  }

  public Optional<IntVector> getIntVector() {
    for (ValueVector v : vectorSchemaRoot.getFieldVectors()) {
      Types.MinorType type = v.getMinorType();
      if (type.equals(Types.MinorType.INT)) return Optional.of((IntVector) v);
    }
    return Optional.empty();
  }

  public Optional<BigIntVector> getBigIntVector() {
    for (ValueVector v : vectorSchemaRoot.getFieldVectors()) {
      Types.MinorType type = v.getMinorType();
      if (type.equals(Types.MinorType.BIGINT)) return Optional.of((BigIntVector) v);
    }
    return Optional.empty();
  }

  public Optional<BaseVariableWidthVector> getVariableWidthVector() {
    for (ValueVector v : vectorSchemaRoot.getFieldVectors()) {
      ArrowType.ArrowTypeID id = v.getField().getType().getTypeID();
      if (id.equals(ArrowType.ArrowTypeID.Binary)) return Optional.of((BaseVariableWidthVector) v);
    }
    return Optional.empty();
  }

  public Optional<VarCharVector> getVarCharVector() {
    for (ValueVector v : vectorSchemaRoot.getFieldVectors()) {
      Types.MinorType type = v.getMinorType();
      if (type.equals(Types.MinorType.VARCHAR)) return Optional.of((VarCharVector) v);
    }
    return Optional.empty();
  }

  public Optional<VarBinaryVector> getVarBinaryVector() {
    for (ValueVector v : vectorSchemaRoot.getFieldVectors()) {
      Types.MinorType type = v.getMinorType();
      if (type.equals(Types.MinorType.VARBINARY)) return Optional.of((VarBinaryVector) v);
    }
    return Optional.empty();
  }

  /**** Private accessor methods to populate the FieldVector(s) from Parquet column values ****/
  private void writeIntColumn(ColumnReader cr, int dmax, FieldVector v, int rows) throws Exception {
    IntVector vector = (IntVector) v;
    vector.setInitialCapacity(rows);
    vector.allocateNew();
    for (int i = 0; i < rows; i++) {
      if (cr.getCurrentDefinitionLevel() == dmax) vector.setSafe(i, cr.getInteger());
      else vector.setNull(i);

      cr.consume();
    }
    vector.setValueCount(rows);
  }

  private void writeLongColumn(ColumnReader cr, int dmax, FieldVector v, int rows)
      throws Exception {
    BigIntVector vector = (BigIntVector) v;
    vector.setInitialCapacity(rows);
    vector.allocateNew();
    for (int i = 0; i < rows; i++) {
      if (cr.getCurrentDefinitionLevel() == dmax) vector.setSafe(i, cr.getLong());
      else vector.setNull(i);

      cr.consume();
    }
    vector.setValueCount(rows);
  }

  private void writeBinaryColumn(ColumnReader cr, int dmax, FieldVector v, int rows)
      throws Exception {
    BaseVariableWidthVector vector = (BaseVariableWidthVector) v;
    vector.setInitialCapacity(rows);
    vector.allocateNew();
    for (int i = 0; i < rows; i++) {
      if (cr.getCurrentDefinitionLevel() == dmax) {
        byte[] data = cr.getBinary().getBytes();
        vector.setIndexDefined(i);
        vector.setValueLengthSafe(i, data.length);
        vector.setSafe(i, data);
      } else vector.setNull(i);

      cr.consume();
    }
    vector.setValueCount(rows);
  }
}
