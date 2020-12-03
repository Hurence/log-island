package com.hurence.logisland.processor.webAnalytics.modele;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RemoveSomeQueryParameterRemover extends AbstractQueryParameterRemover implements QueryParameterRemover {

    final Set<String> parameterToRemove;


    public RemoveSomeQueryParameterRemover(Set<String> parameterToRemove, char keyValueSeparator, char parameterSeparator) {
        super(keyValueSeparator, parameterSeparator);
        this.parameterToRemove = parameterToRemove;
    }

    @Override
    protected List<Map.Entry<String, String>> filterParams(Map<String, String> paramsNameValue) {
        List<Map.Entry<String, String>> paramsNameValueFiltred = paramsNameValue.entrySet().stream()
                .filter(p -> !parameterToRemove.contains(p.getKey()))
                .collect(Collectors.toList());
        return paramsNameValueFiltred;
    }
}
