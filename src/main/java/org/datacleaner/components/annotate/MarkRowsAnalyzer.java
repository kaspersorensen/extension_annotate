package org.datacleaner.components.annotate;

import javax.inject.Named;

import org.datacleaner.api.Analyzer;
import org.datacleaner.api.Concurrent;
import org.datacleaner.api.Configured;
import org.datacleaner.api.Description;
import org.datacleaner.api.HasLabelAdvice;
import org.datacleaner.api.InputColumn;
import org.datacleaner.api.InputRow;
import org.datacleaner.api.Provided;
import org.datacleaner.storage.RowAnnotation;
import org.datacleaner.storage.RowAnnotationFactory;

import com.google.common.base.Strings;

@Named("Mark rows")
@Concurrent(true)
@Description("Allows the user to mark records with a given description, label, tag or annotation. Each record will be collected and counted, but not written to any external store.\n"
        + "This analyzer is particularly useful in combination with other transformations and filtering components - unlocking scenarios where complicated validation logic can be implemented and measured.")
public class MarkRowsAnalyzer implements Analyzer<MarkRowsAnalyzerResult>, HasLabelAdvice {

    @Configured
    InputColumn<?>[] columns;

    @Configured
    String conditionDescription = "...";

    @Provided
    RowAnnotation rowAnnotation;

    @Provided
    RowAnnotationFactory rowAnnotationFactory;

    @Override
    public MarkRowsAnalyzerResult getResult() {
        return new MarkRowsAnalyzerResult(rowAnnotation, rowAnnotationFactory, columns);
    }

    @Override
    public void run(InputRow row, int distinctCount) {
        rowAnnotationFactory.annotate(row, distinctCount, rowAnnotation);
    }

    @Override
    public String getSuggestedLabel() {
        if (!Strings.isNullOrEmpty(conditionDescription)) {
            return "Mark rows as " + conditionDescription;
        }
        return null;
    }

}
