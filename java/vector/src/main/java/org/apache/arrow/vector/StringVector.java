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

package org.apache.arrow.vector;

import static org.apache.arrow.vector.NullCheckingForGet.NULL_CHECKING_ENABLED;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.impl.StringVectorReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

import java.nio.charset.StandardCharsets;

public class StringVector extends BaseVariableWidthVector {
  private final FieldReader reader;

  public StringVector(Field field, BufferAllocator allocator) {
    super(field, allocator);
    reader = new StringVectorReaderImpl(StringVector.this);
  }

  public StringVector(String name, BufferAllocator allocator) {
    this(name, FieldType.nullable(MinorType.STRING.getType()), allocator);
  }

  public StringVector(String name, FieldType fieldType, BufferAllocator allocator) {
    this(new Field(name, fieldType, null), allocator);
  }

  @Override
  public Types.MinorType getMinorType() {
    return MinorType.STRING;
  }

  @Override
  public TransferPair getTransferPair(String s, BufferAllocator bufferAllocator) {
    return new TransferImpl(s, bufferAllocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector valueVector) {
    return new TransferImpl((StringVector) valueVector);
  }

  @Override
  public FieldReader getReader() {
    return reader;
  }

  @Override
  public Object getObject(int index) {
    String res = get(index);
    return res;
  }

  public void setSafe(int index, String value) {
    setSafe(index, value.getBytes());
  }

  public void set(int index, String value) {
    set(index, value.getBytes());
  }

  public String get(int index) {
    assert index >= 0;
    if (NULL_CHECKING_ENABLED && isSet(index) == 0) return null;

    final int start = getStartOffset(index);
    final int len = offsetBuffer.getInt((long) (index + 1) * OFFSET_WIDTH) - start;
    final byte[] arr = new byte[len];
    valueBuffer.getBytes(start, arr, 0, len);

    return new String(arr, StandardCharsets.UTF_8);
  }

  private class TransferImpl implements TransferPair {
    StringVector to;

    public TransferImpl(String ref, BufferAllocator allocator) {
      to = new StringVector(ref, field.getFieldType(), allocator);
    }

    public TransferImpl(StringVector to) {
      this.to = to;
    }

    @Override
    public void transfer() {
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      splitAndTransferTo(startIndex, length, to);
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int fromIndex, int toIndex) {
      to.copyFromSafe(fromIndex, toIndex, StringVector.this);
    }
  }
}
