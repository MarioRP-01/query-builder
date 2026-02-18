package com.enterprise.batch.evaluation.application;

import com.enterprise.batch.evaluation.domain.ConditionRule;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupRunContext {

    private final List<Long> groupIds = new ArrayList<>();
    private final Map<Long, List<ConditionRule>> conditionsByGroup = new HashMap<>();
    private final Map<Long, Map<String, BigDecimal>> aggregatesByGroup = new HashMap<>();

    public void addGroup(Long groupId) {
        groupIds.add(groupId);
    }

    public void addCondition(Long groupId, ConditionRule rule) {
        conditionsByGroup.computeIfAbsent(groupId, k -> new ArrayList<>()).add(rule);
    }

    public void setAggregates(Long groupId, Map<String, BigDecimal> aggregates) {
        aggregatesByGroup.put(groupId, aggregates);
    }

    public List<Long> groupIds() {
        return groupIds;
    }

    public List<ConditionRule> conditionsFor(Long groupId) {
        return conditionsByGroup.getOrDefault(groupId, List.of());
    }

    public Map<String, BigDecimal> aggregatesFor(Long groupId) {
        return aggregatesByGroup.getOrDefault(groupId, Map.of());
    }
}
