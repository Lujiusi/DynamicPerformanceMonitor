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

    private String ruleID;
    private String alertName;
    private JSONObject tags;
    private JSONObject sink;
    private String ruleState;
    private String category;
    private Map<String, String> filters;
    private List<AggregatorFun> aggregatorFun;
    private List<PostAggregatorFun> postAggregatorFun;
    private JSONObject alarmRule;

    public String getRuleID() {
        return ruleID;
    }

    public void setRuleID(String ruleID) {
        this.ruleID = ruleID;
    }

    public String getAlertName() {
        return alertName;
    }

    public void setAlertName(String alertName) {
        this.alertName = alertName;
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
                "ruleID='" + ruleID + '\'' +
                ", alertName='" + alertName + '\'' +
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

    public static class AggregatorFun {

        private JSONObject filters;
        private List<String> groupingKeyNames;
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

        public List<String> getGroupingKeyNames() {
            return groupingKeyNames;
        }

        public void setGroupingKeyNames(List<String> groupingKeyNames) {
            this.groupingKeyNames = groupingKeyNames;
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
                    ", groupingKeyNames=" + groupingKeyNames +
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

        private String feildName;

        public String getFeildName() {
            return feildName;
        }

        public void setFeildName(String feildName) {
            this.feildName = feildName;
        }

        @Override
        public String toString() {
            return "PostFiled{" +
                    "feildName='" + feildName + '\'' +
                    '}';
        }
    }

}