package com.xinye.base;

import com.alibaba.fastjson.JSONObject;

import java.util.List;
import java.util.Map;

/**
 * @author daiwei04@xinye.com
 * @date 2020/11/19 11:27
 * @desc 规则数据模型
 */
public class Rule {

    private Integer ruleID;

    private String ruleState;

    // 对 domain 字段进行过滤
    private List<String> appName;

    // 对 环境进行过滤
    private List<String> env;

    public List<String> getAppName() {
        return appName;
    }

    public void setAppName(List<String> appName) {
        this.appName = appName;
    }

    public List<String> getEnv() {
        return env;
    }

    public void setEnv(List<String> env) {
        this.env = env;
    }

    public Map<String, String> getFilters() {
        return filters;
    }

    public void setFilters(Map<String, String> filters) {
        this.filters = filters;
    }

    // 对 host or port 进行过滤
    private Map<String, String> filters;

    private List<AggregatorFun> aggregatorFun;

    private List<PostAggregatorFun> postAggregatorFun;

    private List<AlarmRule> alarmRule;

    public void setRuleID(Integer ruleID) {
        this.ruleID = ruleID;
    }

    public void setAggregatorFun(List<AggregatorFun> aggregatorFun) {
        this.aggregatorFun = aggregatorFun;
    }

    public void setPostAggregatorFun(List<PostAggregatorFun> postAggregatorFun) {
        this.postAggregatorFun = postAggregatorFun;
    }

    public void setAlarmRule(List<AlarmRule> alarmRule) {
        this.alarmRule = alarmRule;
    }

    public List<PostAggregatorFun> getPostAggregatorFun() {
        return postAggregatorFun;
    }

    public Integer getRuleID() {
        return ruleID;
    }

    public String getRuleState() {
        return ruleState;
    }

    public void setRuleState(String ruleState) {
        this.ruleState = ruleState;
    }

    public List<AggregatorFun> getAggregatorFun() {
        return aggregatorFun;
    }

    public List<AlarmRule> getAlarmRule() {
        return alarmRule;
    }

    @Override
    public String toString() {
        return "Rule{" +
                "ruleID=" + ruleID +
                ", ruleState='" + ruleState + '\'' +
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

        private JSONObject filter;
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

        public JSONObject getFilter() {
            return filter;
        }

        public void setFilter(JSONObject filter) {
            this.filter = filter;
        }

        @Override
        public String toString() {
            return "AggregatorFun{" +
                    "filter=" + filter +
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

        private List<Field> fields;

        private String operator;

        public List<Field> getFields() {
            return fields;
        }

        public void setFields(List<Field> fields) {
            this.fields = fields;
        }

        public String getAliasName() {
            return aliasName;
        }

        public void setAliasName(String aliasName) {
            this.aliasName = aliasName;
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

    public static class Field {

        private String fieldName;

        private JSONObject filters;

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public JSONObject getFilters() {
            return filters;
        }

        public void setFilters(JSONObject filters) {
            this.filters = filters;
        }

        @Override
        public String toString() {
            return "Field{" +
                    "fieldName='" + fieldName + '\'' +
                    ", filters=" + filters +
                    '}';
        }
    }

    public static class AlarmRule {

        private List<AlarmField> fields;

        private Integer alarmId;

        public List<AlarmField> getFields() {
            return fields;
        }

        public void setFields(List<AlarmField> fields) {
            this.fields = fields;
        }

        public Integer getAlarmId() {
            return alarmId;
        }

        public void setAlarmId(Integer alarmId) {
            this.alarmId = alarmId;
        }
    }

    public static class AlarmField {

        private String alarmColumn;

        private String compareOperator;

        private Double target;

        public String getAlarmColumn() {
            return alarmColumn;
        }

        public void setAlarmColumn(String alarmColumn) {
            this.alarmColumn = alarmColumn;
        }

        public String getCompareOperator() {
            return compareOperator;
        }

        public void setCompareOperator(String compareOperator) {
            this.compareOperator = compareOperator;
        }

        public Double getTarget() {
            return target;
        }

        public void setTarget(Double target) {
            this.target = target;
        }

    }

}