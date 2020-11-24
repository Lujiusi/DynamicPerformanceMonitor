package com.xinye.base;

import com.alibaba.fastjson.JSONObject;

import java.util.List;

/**
 * @author daiwei04@xinye.com
 * @date 2020/11/19 11:27
 * @desc 规则数据模型
 */
public class Rule {

    private Integer ruleID;

    private String ruleState;

    //指定分析的类型 是 cpu or jvm
    private String category;

    //分组字段的集合
    private List<String> uniqueKey;
    private JSONObject filters;
    private List<TransFormData> transForm;
    private List<AggregatorFun> aggregatorFun;
    private List<PostAggregatorFun> postAggregatorFun;
    private List<AlarmRule> alarmRule;

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public List<PostAggregatorFun> getPostAggregatorFun() {
        return postAggregatorFun;
    }

    public void setPostAggregatorFun(List<PostAggregatorFun> postAggregatorFun) {
        this.postAggregatorFun = postAggregatorFun;
    }

    public Integer getRuleID() {
        return ruleID;
    }

    public void setRuleID(Integer ruleID) {
        this.ruleID = ruleID;
    }

    public String getRuleState() {
        return ruleState;
    }

    public void setRuleState(String ruleState) {
        this.ruleState = ruleState;
    }

    public List<String> getUniqueKey() {
        return uniqueKey;
    }

    public void setUniqueKey(List<String> uniqueKey) {
        this.uniqueKey = uniqueKey;
    }

    public JSONObject getFilters() {
        return filters;
    }

    public void setFilters(JSONObject filters) {
        this.filters = filters;
    }

    public List<TransFormData> getTransForm() {
        return transForm;
    }

    public void setTransForm(List<TransFormData> transForm) {
        this.transForm = transForm;
    }

    public List<AggregatorFun> getAggregatorFun() {
        return aggregatorFun;
    }

    public void setAggregatorFun(List<AggregatorFun> aggregatorFun) {
        this.aggregatorFun = aggregatorFun;
    }

    public List<AlarmRule> getAlarmRule() {
        return alarmRule;
    }

    public void setAlarmRule(List<AlarmRule> alarmRule) {
        this.alarmRule = alarmRule;
    }

    @Override
    public String toString() {
        return "Rule{" +
                "ruleID=" + ruleID +
                ", ruleState='" + ruleState + '\'' +
                ", uniqueKey=" + uniqueKey +
                ", filters=" + filters +
                ", transForm=" + transForm +
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

    public static class TransFormData {
        private String key;
        private String operator;
        private Object value;
        private String aliasName;

        @Override
        public String toString() {
            return "WashDataFun{" +
                    "key='" + key + '\'' +
                    ", operator='" + operator + '\'' +
                    ", value=" + value +
                    ", aliasName='" + aliasName + '\'' +
                    '}';
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

        public String getAliasName() {
            return aliasName;
        }

        public void setAliasName(String aliasName) {
            this.aliasName = aliasName;
        }
    }

    public static class AggregatorFun {
        private String typeName;
        private List<String> groupingKeyNames;
        private String aggregatorFunctionType;
        private String computeColumn;
        private String resultCode;
        private String defaultValue;
        private Long window;
        private int rating = 1;
        private int delay;
        private String aliasName;
        private String startStep;
        private String endStep;
        private String stepColumn;
        private List<String> distinctColumns;
        private String operator;
        private JSONObject filters;

        public String getResultCode() {
            return resultCode;
        }

        public void setResultCode(String resultCode) {
            this.resultCode = resultCode;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public void setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
        }

        public int getRating() {
            return rating;
        }

        public void setRating(int rating) {
            this.rating = rating;
        }

        public int getDelay() {
            return delay;
        }

        public void setDelay(int delay) {
            this.delay = delay;
        }

        public List<String> getDistinctColumns() {
            return distinctColumns;
        }

        public void setDistinctColumns(List<String> distinctColumns) {
            this.distinctColumns = distinctColumns;
        }

        public String getComputeColumn() {
            return computeColumn;
        }

        public String getStartStep() {
            return startStep;
        }

        public void setStartStep(String startStep) {
            this.startStep = startStep;
        }

        public void setEndStep(String endStep) {
            this.endStep = endStep;
        }

        public String getEndStep() {
            return endStep;
        }

        public String getStepColumn() {
            return stepColumn;
        }

        public void setStepColumn(String stepColumn) {
            this.stepColumn = stepColumn;
        }

        public String getOperator() {
            return operator;
        }

        @Override
        public String toString() {
            return "AggregatorFun{" +
                    "typeName='" + typeName + '\'' +
                    ", groupingKeyNames=" + groupingKeyNames +
                    ", aggregatorFunctionType='" + aggregatorFunctionType + '\'' +
                    ", computeColumn='" + computeColumn + '\'' +
                    ", resultCode='" + resultCode + '\'' +
                    ", defaultValue='" + defaultValue + '\'' +
                    ", window=" + window +
                    ", rating=" + rating +
                    ", delay=" + delay +
                    ", aliasName='" + aliasName + '\'' +
                    ", startStep='" + startStep + '\'' +
                    ", endStep='" + endStep + '\'' +
                    ", stepColumn='" + stepColumn + '\'' +
                    ", distinctColumns=" + distinctColumns +
                    ", operator='" + operator + '\'' +
                    ", filters=" + filters +
                    '}';
        }

        public JSONObject getFilters() {
            return filters;
        }

        public void setFilters(JSONObject filters) {
            this.filters = filters;
        }

        public void setOperator(String operator) {
            this.operator = operator;
        }

        public void setComputeColumn(String computeColumn) {
            this.computeColumn = computeColumn;
        }

        public String getTypeName() {
            return typeName;
        }

        public void setTypeName(String typeName) {
            this.typeName = typeName;
        }

        public List<String> getGroupingKeyNames() {
            return groupingKeyNames;
        }

        public void setGroupingKeyNames(List<String> groupingKeyNames) {
            this.groupingKeyNames = groupingKeyNames;
        }

        public String getAggregatorFunctionType() {
            return aggregatorFunctionType;
        }

        public void setAggregatorFunctionType(String aggregatorFunctionType) {
            this.aggregatorFunctionType = aggregatorFunctionType;
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
    }


    public static class PostAggregatorFun {
        private List<Field> fields;
        private String operator;
        private String aliasName;


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
        private String postAgg;

        public String getPostAgg() {
            return postAgg;
        }

        public void setPostAgg(String postAgg) {
            this.postAgg = postAgg;
        }

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
                    ", postAgg='" + postAgg + '\'' +
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
