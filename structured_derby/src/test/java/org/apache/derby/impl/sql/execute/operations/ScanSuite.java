package org.apache.derby.impl.sql.execute.operations;

import org.junit.experimental.categories.Categories;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Scott Fines
 *         Created on: 2/24/13
 */

@RunWith(Categories.class)
@Categories.ExcludeCategory(OperationCategories.Transactional.class)
@Suite.SuiteClasses({
        DistinctScanOperationTest.class,
        ProjectRestrictOperationTest.class,
        TableScanOperationTest.class,
        UnionOperationTest.class,
        SortOperationTest.class,
        MetatablesTest.class,
        TimeTableScanTest.class,
        RowCountOperationTest.class
})
public class ScanSuite { }
