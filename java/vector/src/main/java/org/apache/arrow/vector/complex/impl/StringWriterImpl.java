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

package org.apache.arrow.vector.complex.impl;

import org.apache.arrow.vector.StringVector;
import org.apache.arrow.vector.types.pojo.Field;

public class StringWriterImpl extends AbstractFieldWriter {
  final StringVector vector;

  public StringWriterImpl(StringVector vector) {
    this.vector = vector;
  }

  @Override
  public void allocate() {
    this.vector.allocateNew();
  }

  @Override
  public void clear() {
    this.vector.clear();
  }

  @Override
  public void close() {
    this.vector.close();
  }

  @Override
  public Field getField() {
    return this.vector.getField();
  }

  @Override
  public int getValueCapacity() {
    return this.vector.getValueCapacity();
  }

  protected int idx() {
    return super.idx();
  }
}
