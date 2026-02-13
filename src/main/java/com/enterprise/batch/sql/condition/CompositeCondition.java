package com.enterprise.batch.sql.condition;

import com.enterprise.batch.sql.param.ParameterBinder;

import java.util.List;
import java.util.stream.Collectors;

public class CompositeCondition implements Condition {

    public enum Logic { AND, OR }

    private final Logic logic;
    private final List<Condition> children;

    public CompositeCondition(Logic logic, List<Condition> children) {
        this.logic = logic;
        this.children = children;
    }

    @Override
    public String toSql(ParameterBinder binder) {
        String delimiter = logic == Logic.AND ? " AND " : " OR ";
        String joined = children.stream()
                .map(c -> c.toSql(binder))
                .collect(Collectors.joining(delimiter));
        return "(" + joined + ")";
    }
}
