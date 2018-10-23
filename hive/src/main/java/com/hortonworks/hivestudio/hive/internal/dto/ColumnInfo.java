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


package com.hortonworks.hivestudio.hive.internal.dto;

import lombok.Data;

import java.util.Objects;

@Data
public class ColumnInfo {
  private String name;
  private String type;
  private Integer precision;
  private Integer scale;
  private String comment;

  public ColumnInfo(){
    // for json de-serialization
  }

  public ColumnInfo(String name, String type, Integer precision, Integer scale, String comment) {
    this.name = name;
    this.type = type;
    this.precision = precision;
    this.scale = scale;
    this.comment = comment;
  }

  public ColumnInfo(String name, String type, String comment) {
    this(name, type, null, null, comment);
  }

  public ColumnInfo(String name, String type, Integer precision, String comment) {
    this(name, type, precision, null, comment);
  }

  public ColumnInfo(String name, String type, Integer precision, Integer scale) {
    this(name, type, precision, scale, null);
  }

  public ColumnInfo(String name, String type, Integer precision) {
    this(name, type, precision, null, null);
  }

  public ColumnInfo(String name, String type) {
    this(name, type, null, null, null);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ColumnInfo that = (ColumnInfo) o;
    return ((name == that.name) || (name != null && name.equalsIgnoreCase(that.name))) &&
        ((type == that.type) || (type != null && type.equalsIgnoreCase(that.type))) &&
        Objects.equals(precision, that.precision) &&
        Objects.equals(scale, that.scale) &&
        Objects.equals(comment, that.comment);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
