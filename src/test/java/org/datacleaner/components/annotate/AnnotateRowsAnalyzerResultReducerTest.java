package org.datacleaner.components.annotate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;

import org.datacleaner.api.InputColumn;
import org.datacleaner.data.MockInputColumn;
import org.datacleaner.data.MockInputRow;
import org.datacleaner.storage.InMemoryRowAnnotationFactory2;
import org.datacleaner.storage.RowAnnotation;
import org.datacleaner.storage.RowAnnotationFactory;
import org.junit.Test;

public class AnnotateRowsAnalyzerResultReducerTest {

    private final AnnotateRowsAnalyzerResultReducer reducer = new AnnotateRowsAnalyzerResultReducer();

    @Test
    public void testReduceNone() throws Exception {
        final AnnotateRowsAnalyzerResult result = reducer.reduce(Collections.<AnnotateRowsAnalyzerResult> emptyList());
        assertEquals(0, result.getTotalRowCount());
        assertTrue(result.getSampleRows().isEmpty());
    }

    @Test
    public void testReduceSums() throws Exception {
        final AnnotateRowsAnalyzerResult result1 = createResult();
        final AnnotateRowsAnalyzerResult result2 = createResult();

        final AnnotateRowsAnalyzerResult result = reducer.reduce(Arrays.asList(result1, result2));
        assertEquals(2, result.getAnnotatedRowCount());
        assertTrue(result.getSampleRows().isEmpty());
    }

    private AnnotateRowsAnalyzerResult createResult() {
        final RowAnnotationFactory annotationFactory = new InMemoryRowAnnotationFactory2();
        final RowAnnotation annotation = annotationFactory.createAnnotation();
        final InputColumn<?> col1 = new MockInputColumn<String>("foo");
        final InputColumn<?> col2 = new MockInputColumn<Number>("bar");
        final InputColumn<?>[] highlightedColumns = new InputColumn[] { col1, col2 };

        annotationFactory.annotate(new MockInputRow().put(col1, "hello world").put(col2, 42), annotation);

        return new AnnotateRowsAnalyzerResult(annotation, annotationFactory, highlightedColumns);
    }
}
