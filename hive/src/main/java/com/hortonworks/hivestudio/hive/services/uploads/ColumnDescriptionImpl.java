/*
 *
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *   LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *   FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *   DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *   DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *   OR LOSS OR CORRUPTION OF DATA.
 *
 */
package com.hortonworks.hivestudio.hive.services.uploads;

import com.hortonworks.hivestudio.hive.client.ColumnDescription;

import java.io.Serializable;

/**
 * implementation of ColumnDescription which also includes scale and precision.
 */
public class ColumnDescriptionImpl implements ColumnDescription, Serializable {
  private String name;
  private String type;
  private int position;
  /**
   * can be null
   */
  private Integer precision;
  /**
   * can be null
   */
  private Integer scale;

  public ColumnDescriptionImpl() {
  }

  public ColumnDescriptionImpl(String name, String type, int position) {
    this.name = name;
    this.type = type;
    this.position = position;
  }

  public ColumnDescriptionImpl(String name, String type, int position, int precision) {
    this.name = name;
    this.type = type;
    this.position = position;
    this.precision = precision;
  }

  public ColumnDescriptionImpl(String name, String type, int position, int precision, int scale) {
    this.name = name;
    this.type = type;
    this.position = position;
    this.precision = precision;
    this.scale = scale;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public int getPosition() {
    return this.position;
  }

  public Integer getPrecision() {
    return precision;
  }

  public Integer getScale() {
    return scale;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ColumnDescriptionImpl that = (ColumnDescriptionImpl) o;

    if (position != that.position) return false;
    if (!name.equals(that.name)) return false;
    return type.equals(that.type);

  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + type.hashCode();
    result = 31 * result + position;
    return result;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("ColumnDescriptionImpl[")
            .append("name : ").append(name)
            .append(", type : " + type)
            .append(", position : " + position)
            .append(", precision : " + precision)
            .append(", scale : " + scale)
            .append("]").toString();
  }
}
