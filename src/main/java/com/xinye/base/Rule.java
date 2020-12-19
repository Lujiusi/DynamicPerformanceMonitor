package com.xinye.base;

import com.alibaba.fastjson.JSONObject;

import java.util.List;
import java.util.Map;

/**
 * 规则数据模型
 *
 * @author daiwei04@xinye.com
 * @since 2020/11/19 11:27
 */
public class Rule {

    private Integer ruleID;
    private JSONObject tags;
    private JSONObject sink;
    private String ruleState;
    private String category;
    private Map<String, String> filters;
    private List<AggregatorFun> aggregatorFun;
    private List<PostAggregatorFun> postAggregatorFun;
    private JSONObject alarmRule;

    public Integer getRuleID() {
        return ruleID;
    }

    public void setRuleID(Integer ruleID) {
        this.ruleID = ruleID;
    }

    public JSONObject getTags() {
        return tags;
    }

    public void setTags(JSONObject tags) {
        this.tags = tags;
    }

    public JSONObject getSink() {
        return sink;
    }

    public void setSink(JSONObject sink) {
        this.sink = sink;
    }

    public String getRuleState() {
        return ruleState;
    }

    public void setRuleState(String ruleState) {
        this.ruleState = ruleState;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Map<String, String> getFilters() {
        return filters;
    }

    public void setFilters(Map<String, String> filters) {
        this.filters = filters;
    }

    public List<AggregatorFun> getAggregatorFun() {
        return aggregatorFun;
    }

    public void setAggregatorFun(List<AggregatorFun> aggregatorFun) {
        this.aggregatorFun = aggregatorFun;
    }

    public List<PostAggregatorFun> getPostAggregatorFun() {
        return postAggregatorFun;
    }

    public void setPostAggregatorFun(List<PostAggregatorFun> postAggregatorFun) {
        this.postAggregatorFun = postAggregatorFun;
    }

    public JSONObject getAlarmRule() {
        return alarmRule;
    }

    public void setAlarmRule(JSONObject alarmRule) {
        this.alarmRule = alarmRule;
    }

    @Override
    public String toString() {
        return "Rule{" +
                "ruleID=" + ruleID +
                ", tags=" + tags +
                ", sink=" + sink +
                ", ruleState='" + ruleState + '\'' +
                ", category='" + category + '\'' +
                ", filters=" + filters +
                ", aggregatorFun=" + aggregatorFun +
                ", postAggregatorFun=" + postAggregatorFun +
                ", alarmRule=" + alarmRule +
                '}';
    }

    public static class Filter {

        private String key;
        private String operator;
        private Object value;
        private String filterType;

        @Override
        public String toString() {
            return "Filter{" +
                    "key='" + key + '\'' +
                    ", operator='" + operator + '\'' +
                    ", value=" + value +
                    ", filterType='" + filterType + '\'' +
                    '}';
        }

        public String getFilterType() {
            return filterType;
        }

        public void setFilterType(String filterType) {
            this.filterType = filterType;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getOperator() {
            return operator;
        }

        public void setOperator(String operator) {
            this.operator = operator;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

    }

    public static class AggregatorFun {

        private JSONObject filters;
        private List<String> groupingNames;
        private String aggregatorFunctionType;
        private String computeColumn;
        private String datasource;
        private Long window;
        private String aliasName;

        public String getAggregatorFunctionType() {
            return aggregatorFunctionType;
        }

        public void setAggregatorFunctionType(String aggregatorFunctionType) {
            this.aggregatorFunctionType = aggregatorFunctionType;
        }

        public List<String> getGroupingNames() {
            return groupingNames;
        }

        public void setGroupingNames(List<String> groupingNames) {
            this.groupingNames = groupingNames;
        }

        public String getComputeColumn() {
            return computeColumn;
        }

        public void setComputeColumn(String computeColumn) {
            this.computeColumn = computeColumn;
        }

        public String getDatasource() {
            return datasource;
        }

        public void setDatasource(String datasource) {
            this.datasource = datasource;
        }

        public Long getWindow() {
            return window;
        }

        public void setWindow(Long window) {
            this.window = window;
        }

        public String getAliasName() {
            return aliasName;
        }

        public void setAliasName(String aliasName) {
            this.aliasName = aliasName;
        }

        public JSONObject getFilters() {
            return filters;
        }

        public void setFilters(JSONObject filter) {
            this.filters = filter;
        }

        @Override
        public String toString() {
            return "AggregatorFun{" +
                    "filters=" + filters +
                    ", groupingNames=" + groupingNames +
                    ", aggregatorFunctionType='" + aggregatorFunctionType + '\'' +
                    ", computeColumn='" + computeColumn + '\'' +
                    ", datasource='" + datasource + '\'' +
                    ", window=" + window +
                    ", aliasName='" + aliasName + '\'' +
                    '}';
        }
    }

    public static class PostAggregatorFun {

        private String aliasName;

        private List<PostFiled> fields;

        private String operator;

        public String getAliasName() {
            return aliasName;
        }

        public void setAliasName(String aliasName) {
            this.aliasName = aliasName;
        }

        public List<PostFiled> getFields() {
            return fields;
        }

        public void setFields(List<PostFiled> fields) {
            this.fields = fields;
        }

        public String getOperator() {
            return operator;
        }

        public void setOperator(String operator) {
            this.operator = operator;
        }

        @Override
        public String toString() {
            return "PostAggregatorFun{" +
                    "fields=" + fields +
                    ", operator='" + operator + '\'' +
                    ", aliasName='" + aliasName + '\'' +
                    '}';
        }

    }

    public static class PostFiled {

        private String fieldName;

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public String toString() {
            return "PostFiled{" +
                    "fieldName='" + fieldName + '\'' +
                    '}';
        }
    }

}