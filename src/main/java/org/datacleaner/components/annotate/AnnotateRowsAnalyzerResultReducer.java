package org.datacleaner.components.annotate;

import java.util.Collection;

import org.datacleaner.api.AnalyzerResultReducer;
import org.datacleaner.api.InputColumn;
import org.datacleaner.storage.RowAnnotationImpl;

public class AnnotateRowsAnalyzerResultReducer implements AnalyzerResultReducer<AnnotateRowsAnalyzerResult> {

    @Override
    public AnnotateRowsAnalyzerResult reduce(Collection<? extends AnnotateRowsAnalyzerResult> results) {
        InputColumn<?>[] highlightedColumns = null;
        final RowAnnotationImpl annotation = new RowAnnotationImpl();
        for (AnnotateRowsAnalyzerResult result : results) {
            final int annotatedRowCount = result.getAnnotatedRowCount();
            annotation.incrementRowCount(annotatedRowCount);
            if (highlightedColumns == null || highlightedColumns.length == 0) {
                highlightedColumns = result.getHighlightedColumns();
            }
        }
        return new AnnotateRowsAnalyzerResult(annotation, null, highlightedColumns);
    }

}
