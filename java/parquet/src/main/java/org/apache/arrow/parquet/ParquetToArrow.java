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

import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.arrow.vector.ipc.ArrowFileWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.util.*;

import org.apache.arrow.parquet.utils.*;

/**
 * Created by atr on 19.12.17.
 */

public class ParquetToArrow {
    private Configuration conf;
    private MessageType parquetSchema;
    private ParquetFileReader parquetFileReader;
    private ParquetMetadata parquetFooter;

    private Path arrowPath;
    private Schema arrowSchema;
    private VectorSchemaRoot arrowVectorSchemaRoot;
    private ArrowFileWriter arrowFileWriter;
    private RootAllocator ra = null;

    private List<Double> time = new ArrayList<>();
    private Long t0, t1, t2, t3, t4, t5, t6, t7 = 0L;

    public ParquetToArrow(){
        t0 = System.nanoTime();
        this.conf = new Configuration();
        this.ra = new RootAllocator(Integer.MAX_VALUE);
        this.arrowPath = new Path("arrowOutput/");
        t1 = System.nanoTime();
        time.add((t1 - t0) / 1e9d);
    }

    public void setParquetInputFile(String parquetFile) throws Exception {
        t2 = System.nanoTime();
        Path parqutFilePath = new Path(parquetFile);
        this.parquetFooter = ParquetFileReader.readFooter(conf,
                parqutFilePath,
                ParquetMetadataConverter.NO_FILTER);

        FileMetaData mdata = this.parquetFooter.getFileMetaData();
        this.parquetSchema = mdata.getSchema();
        this.parquetFileReader = new ParquetFileReader(conf,
                mdata,
                parqutFilePath,
                this.parquetFooter.getBlocks(),
                this.parquetSchema.getColumns());
        t3 = System.nanoTime();
        t4 = System.nanoTime();
        makeArrowSchema();
        setArrowFileWriter(convertParquetToArrowFileName(parqutFilePath));
        t5 = System.nanoTime();

        time.add((t3 - t2) / 1e9d);
        time.add((t5 - t4) / 1e9d);
    }

    private String convertParquetToArrowFileName(Path parquetNamePath){
        String oldsuffix = ".parquet";
        String newSuffix = ".arrow";
        String fileName = parquetNamePath.getName();
        if (!fileName.endsWith(oldsuffix)) {
            return fileName + newSuffix;
        }
        return fileName.substring(0, fileName.length() - oldsuffix.length()) + newSuffix;
    }

    private void makeArrowSchema() throws Exception {
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        StringBuilder sb = new StringBuilder();
        for(ColumnDescriptor col: this.parquetSchema.getColumns()){
            sb.setLength(0);
            String[] p = col.getPath();
            for(String px: p)
                sb.append(px);
            switch (col.getType()) {
                case INT32 :
                    childrenBuilder.add(new Field(sb.toString(),
                            FieldType.nullable(new ArrowType.Int(32, true)), null));
                    break;
                case INT64 :
                    childrenBuilder.add(new Field(sb.toString(),
                            FieldType.nullable(new ArrowType.Int(64, true)), null));
                    break;
                case DOUBLE :
                    childrenBuilder.add(new Field(sb.toString(),
                            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
                    break;
                case BINARY :
                    childrenBuilder.add(new Field(sb.toString(),
                            FieldType.nullable(new ArrowType.Binary()), null));
                    break;
                // has float
                //case FLOAT:
                default : throw new Exception(" NYI " + col.getType());
            }
        }
        this.arrowSchema = new Schema(childrenBuilder.build(), null);
        //System.out.println("Arrow Schema is " + this.arrowSchema.toString());
    }

    private void setArrowFileWriter(String arrowFileName) throws Exception{
        String arrowFullPath = this.arrowPath.toUri().toString() + "/" + arrowFileName;
        System.out.println("Creating a file with name : " + arrowFullPath);
        // create the file stream on HDFS
        Path path = new Path(arrowFullPath);
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        // default is to over-write
        FSDataOutputStream file = fs.create(new Path(path.toUri().getRawPath()));
        this.arrowVectorSchemaRoot = VectorSchemaRoot.create(this.arrowSchema, this.ra);
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();

        boolean writeLocalFile = true;  /* Condition never changes 18-02-2021*/
    }

    public void process() throws Exception {
        t6 = System.nanoTime();
        PageReadStore pageReadStore = null;
        List<ColumnDescriptor> colDesc = parquetSchema.getColumns();
        List<FieldVector> fieldVectors = this.arrowVectorSchemaRoot.getFieldVectors();
        int size = colDesc.size();
        DumpGroupConverter conv = new DumpGroupConverter();
        this.arrowFileWriter.start();
        pageReadStore = parquetFileReader.readNextRowGroup();
        while (pageReadStore != null) {
            ColumnReadStoreImpl colReader = new ColumnReadStoreImpl(pageReadStore, conv,
                    this.parquetSchema, this.parquetFooter.getFileMetaData().getCreatedBy());
            if(pageReadStore.getRowCount() > Integer.MAX_VALUE)
                throw new Exception(" More than Integer.MAX_VALUE is not supported " + pageReadStore.getRowCount());
            int rows = (int) pageReadStore.getRowCount();
            // this batch of Arrow contains these many records
            this.arrowVectorSchemaRoot.setRowCount(rows);

            int i = 0;
            while (i < size){
                ColumnDescriptor col = colDesc.get(i);
                switch(col.getType()) {

                    case INT32: writeIntColumn(colReader.getColumnReader(col),
                            col.getMaxDefinitionLevel(),
                            fieldVectors.get(i),
                            rows);
                    break;

                    case INT64: writeLongColumn(colReader.getColumnReader(col),
                            col.getMaxDefinitionLevel(),
                            fieldVectors.get(i),
                            rows);
                    break;

                    case DOUBLE: writeDoubleColumn(colReader.getColumnReader(col),
                            col.getMaxDefinitionLevel(),
                            fieldVectors.get(i),
                            rows);
                    break;

                    case BINARY: writeBinaryColumn(colReader.getColumnReader(col),
                            col.getMaxDefinitionLevel(),
                            fieldVectors.get(i),
                            rows);
                    break;

                    default : throw new Exception(" NYI " + col.getType());
                }
                i+=1;
            }
            pageReadStore = parquetFileReader.readNextRowGroup();
            this.arrowFileWriter.writeBatch();
        }
        this.arrowFileWriter.end();
        this.arrowFileWriter.close();
        t7 = System.nanoTime();
        time.add((t7 - t6) / 1e9d);
    }

    public List<Double> getTime(){ return time; }

    private void writeIntColumn(ColumnReader creader, int dmax, FieldVector fieldVector, int rows) throws Exception {
        IntVector intVector = (IntVector) fieldVector;
        intVector.setInitialCapacity(rows);
        intVector.allocateNew();

        for(int i = 0; i < rows; i++) {
            if(creader.getCurrentDefinitionLevel() == dmax){
                intVector.setSafe(i, 1, creader.getInteger());
            } else {
                intVector.setNull(i);
            }
            creader.consume();
        }
        intVector.setValueCount(rows);
    }

    private void writeLongColumn(ColumnReader creader, int dmax, FieldVector fieldVector, int rows) throws Exception {
        BigIntVector bigIntVector = (BigIntVector) fieldVector;
        bigIntVector.setInitialCapacity(rows);
        bigIntVector.allocateNew();

        for(int i = 0; i < rows; i++) {
            if(creader.getCurrentDefinitionLevel() == dmax){
                bigIntVector.setSafe(i, 1, creader.getLong());
            } else {
                bigIntVector.setNull(i);
            }
            creader.consume();
        }
        bigIntVector.setValueCount(rows);
    }

    private void writeDoubleColumn(ColumnReader creader, int dmax, FieldVector fieldVector, int rows) throws Exception {
        Float8Vector float8Vector  = (Float8Vector ) fieldVector;
        float8Vector.setInitialCapacity((int) rows);
        float8Vector.allocateNew();

        for(int i = 0; i < rows; i++) {
            if(creader.getCurrentDefinitionLevel() == dmax){
                float8Vector.setSafe(i, 1, creader.getDouble());
            } else {
                float8Vector.setNull(i);
            }
            creader.consume();
        }
        float8Vector.setValueCount(rows);
    }

    private void writeBinaryColumn(ColumnReader creader, int dmax, FieldVector fieldVector, int rows) throws Exception {
        VarBinaryVector varBinaryVector  = (VarBinaryVector) fieldVector;
        varBinaryVector.setInitialCapacity((int) rows);
        varBinaryVector.allocateNew();

        for(int i = 0; i < rows; i++) {
            if(creader.getCurrentDefinitionLevel() == dmax){
                byte[] data = creader.getBinary().getBytes();
                varBinaryVector.setIndexDefined(i);
                varBinaryVector.setValueLengthSafe(i, data.length);
                varBinaryVector.setSafe(i, data);
            } else {
                varBinaryVector.setNull(i);
            }
            creader.consume();
        }
        varBinaryVector.setValueCount(rows);
    }

    /**
     * Returns the VectorSchemaRoot for later use
     *
     * author:  ffiorini
     * date:    17-01-2021
     */
    public VectorSchemaRoot getSchemaRoot()
    {
        return this.arrowVectorSchemaRoot;
    }

    /**
     * Support for returning individual vectors from VectorSchemaRoot
     */

    /**
     *
     * @return
     */
    public Optional<IntVector> getIntegerVector()
    {
        for (ValueVector v : this.arrowVectorSchemaRoot.getFieldVectors())
        {
            ArrowType.ArrowTypeID id = v.getField().getType().getTypeID();
            if (id.equals(ArrowType.ArrowTypeID.Int))
            {
                return Optional.of((IntVector) v);
            }
        }
        return Optional.empty();
    }

    public Optional<BigIntVector> getInteger64Vector()
    {
        for (ValueVector v : this.arrowVectorSchemaRoot.getFieldVectors())
        {
            ArrowType.ArrowTypeID id = v.getField().getType().getTypeID();
            if (id.equals(ArrowType.ArrowTypeID.Int))
            {
                return Optional.of((BigIntVector) v);
            }
        }
        return Optional.empty();
    }

    public Optional<BaseVariableWidthVector> getBinaryVector()
    {
        for (ValueVector v : this.arrowVectorSchemaRoot.getFieldVectors())
        {
            ArrowType.ArrowTypeID id = v.getField().getType().getTypeID();
            if (id.equals(ArrowType.ArrowTypeID.Binary))
            {
                return Optional.of((BaseVariableWidthVector) v);
            }
        }
        return Optional.empty();
    }
}