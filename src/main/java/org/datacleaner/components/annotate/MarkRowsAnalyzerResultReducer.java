package org.datacleaner.components.annotate;

import java.util.Collection;

import org.datacleaner.api.AnalyzerResultReducer;
import org.datacleaner.api.InputColumn;
import org.datacleaner.storage.RowAnnotationImpl;

public class MarkRowsAnalyzerResultReducer implements AnalyzerResultReducer<MarkRowsAnalyzerResult> {

    @Override
    public MarkRowsAnalyzerResult reduce(Collection<? extends MarkRowsAnalyzerResult> results) {
        InputColumn<?>[] highlightedColumns = null;
        final RowAnnotationImpl annotation = new RowAnnotationImpl();
        for (MarkRowsAnalyzerResult result : results) {
            final int annotatedRowCount = result.getAnnotatedRowCount();
            annotation.incrementRowCount(annotatedRowCount);
            if (highlightedColumns == null || highlightedColumns.length == 0) {
                highlightedColumns = result.getHighlightedColumns();
            }
        }
        return new MarkRowsAnalyzerResult(annotation, null, highlightedColumns);
    }

}
