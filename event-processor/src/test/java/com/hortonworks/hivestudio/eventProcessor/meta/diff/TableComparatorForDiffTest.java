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
package com.hortonworks.hivestudio.eventProcessor.meta.diff;

import com.hortonworks.hivestudio.common.entities.Column;
import com.hortonworks.hivestudio.common.repository.ColumnRepository;
import org.hamcrest.beans.SamePropertyValuesAs;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;

public class TableComparatorForDiffTest {

//  Assumes actual object of ColumnComparatorForDiff so it actually tests ColumnComparatorForDiff as well in some sense
  ColumnComparatorForDiff columnComparatorForDiff = new ColumnComparatorForDiff();

  TableComparatorForDiff tableComprator;

  @Mock
  Provider<ColumnRepository> columnRepositoryProvider;

  @Before
  public void setUp(){
    MockitoAnnotations.initMocks(this);
    tableComprator = new TableComparatorForDiff(columnComparatorForDiff, columnRepositoryProvider);
  }

  private Column createColumn(Integer id, String name, String datatype) {
    Column column = new Column();
    column.setId(id);
    column.setName(name);
    column.setDatatype(datatype);
    return column;
  }

  private Column createColumn(Integer id, String name, String datatype, int precission) {
    Column column = createColumn(id, name, datatype);
    column.setPrecision(precission);
    return column;
  }
  private Column createColumn(Integer id, String name, String datatype, String comment) {
    Column column = createColumn(id, name, datatype);
    column.setComment(comment);
    return column;
  }

  @Test
  public void findColumnsDiff() {

    Collection<Column> originalColumns = new ArrayList<>();
    Collection<Column> newColumns = new ArrayList<>();

    String col1 = "col1";
    String col2 = "col2";
    String col3 = "col3";
    String col4 = "col4";
    String col5 = "col5";

    String varchar = "varchar";
    String string = "string";
    String intType = "int";
    String doubleType = "double";
    String comment1 = "somecomment";
    String comment2 = "somecomment";

    int precision10 = 10;
    int precision5 = 5;

    Column originalCol1 = createColumn(1, col1, string);
    Column originalCol2 = createColumn(2, col2, varchar, precision10);
    Column originalCol3 = createColumn(3, col3, intType);
    Column originalCol4 = createColumn(4, col4, doubleType);

    Column newCol1 = createColumn(null, col1, varchar, precision5);  // --> updated type and precision
    Column newCol2 = createColumn(null, col2, varchar, precision10); // --> unchanged
//    Column newCol3 = createColumn(null, "col3", "int");     // --> deleted by not including.
    Column newCol4 = createColumn(null, col4, doubleType, comment1); // --> updated comment
    Column newCol5 = createColumn(null, col5, string, comment2); // --> added column

    Column expectedCol1 = createColumn(1, col1, varchar, precision5);
    Column expectedCol4 = createColumn(4, col4, doubleType, comment1);
    Column expectedCol5 = createColumn(null, col5, string, comment2);

    originalColumns.add(originalCol1);
    originalColumns.add(originalCol2);
    originalColumns.add(originalCol3);
    originalColumns.add(originalCol4);

    newColumns.add(newCol1);
    newColumns.add(newCol2);
    newColumns.add(newCol4);
    newColumns.add(newCol5);

    Optional<ColumnsDiff> columnsDiff = tableComprator.findColumnsDiff(originalColumns, newColumns);

    Assert.assertTrue("Diff object should not be empty.", columnsDiff.isPresent());

    Assert.assertTrue("Added columns collection should not be empty.", columnsDiff.get().getColumnsAdded().isPresent());
    Assert.assertEquals("Added columns collection should have exactly one element.", 1, columnsDiff.get().getColumnsAdded().get().size());
    Collection<Column> addedColumns = columnsDiff.get().getColumnsAdded().get();

    Assert.assertThat(addedColumns, hasSize(1));
    Assert.assertThat(addedColumns, hasItem(new SamePropertyValuesAs<>(expectedCol5)));

    Assert.assertTrue("Updated columns collection should not be empty.", columnsDiff.get().getColumnsUpdated().isPresent());
    Collection<Column> updatedColumns = columnsDiff.get().getColumnsUpdated().get();
    Assert.assertThat(updatedColumns, hasSize(2));
//    This is be true when all the columns matches but may return out of order mismatch, because collections are compared.
    Assert.assertThat(updatedColumns, hasItems(new SamePropertyValuesAs<>(expectedCol1), new SamePropertyValuesAs<>(expectedCol4)));

    Assert.assertTrue("Dropped columns collection should not be empty.", columnsDiff.get().getColumnsDropped().isPresent());
    Collection<Column> droppedColumns = columnsDiff.get().getColumnsDropped().get();
    Assert.assertThat(droppedColumns, hasItems(new SamePropertyValuesAs<>(originalCol3)));


    Assert.assertTrue("Unchanged columns collection should not be empty.", columnsDiff.get().getColumnsUnchanged().isPresent());
    Collection<Column> unchangedColumns = columnsDiff.get().getColumnsUnchanged().get();
    Assert.assertThat(unchangedColumns, hasItems(new SamePropertyValuesAs<>(originalCol2)));
  }
}