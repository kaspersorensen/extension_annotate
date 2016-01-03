package org.datacleaner.components.annotate;

import javax.inject.Named;

import org.datacleaner.api.Analyzer;
import org.datacleaner.api.Concurrent;
import org.datacleaner.api.Configured;
import org.datacleaner.api.HasLabelAdvice;
import org.datacleaner.api.InputColumn;
import org.datacleaner.api.InputRow;
import org.datacleaner.api.Provided;
import org.datacleaner.storage.RowAnnotation;
import org.datacleaner.storage.RowAnnotationFactory;

import com.google.common.base.Strings;

@Named("Annotate rows")
@Concurrent(true)
public class AnnotateRowsAnalyzer implements Analyzer<AnnotateRowsAnalyzerResult>, HasLabelAdvice {

    @Configured
    InputColumn<?>[] columns;

    @Configured(required = false)
    String conditionDescription;

    @Provided
    RowAnnotation rowAnnotation;

    @Provided
    RowAnnotationFactory rowAnnotationFactory;

    @Override
    public AnnotateRowsAnalyzerResult getResult() {
        return new AnnotateRowsAnalyzerResult(rowAnnotation, rowAnnotationFactory, columns);
    }

    @Override
    public void run(InputRow row, int distinctCount) {
        rowAnnotationFactory.annotate(row, distinctCount, rowAnnotation);
    }

    @Override
    public String getSuggestedLabel() {
        if (!Strings.isNullOrEmpty(conditionDescription)) {
            return conditionDescription;
        }
        return null;
    }

}
