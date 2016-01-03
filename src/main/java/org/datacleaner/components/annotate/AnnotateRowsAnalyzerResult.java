package org.datacleaner.components.annotate;

import org.datacleaner.api.Description;
import org.datacleaner.api.Distributed;
import org.datacleaner.api.InputColumn;
import org.datacleaner.api.Metric;
import org.datacleaner.result.AnnotatedRowsResult;
import org.datacleaner.storage.RowAnnotation;
import org.datacleaner.storage.RowAnnotationFactory;

@Description("Annotated rows")
@Distributed(reducer = AnnotateRowsAnalyzerResultReducer.class)
public class AnnotateRowsAnalyzerResult extends AnnotatedRowsResult {

    private static final long serialVersionUID = 1L;

    public AnnotateRowsAnalyzerResult(RowAnnotation annotation, RowAnnotationFactory annotationFactory,
            InputColumn<?>[] highlightedColumns) {
        super(annotation, annotationFactory, highlightedColumns);
    }

    @Metric(order = 1, value = "Row count")
    public int getTotalRowCount() {
        return getAnnotatedRowCount();
    }
}
