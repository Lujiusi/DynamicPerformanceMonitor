//package com.xinye.base;
//
///**
// * @author daiwei04@xinye.com
// * @date 2020/12/1 10:00
// * @desc
// */
//public class InfluxdbMetric {
//
//    public static String toInfluxDBLineProtocol(Metric metricData) {
//        StringBuffer builder = new StringBuffer();
//        builder.append(metricData.getMetric());
//        Map<String, String> tags = metricData.getTags();
//        if (!tags.isEmpty()) {
//            for (Map.Entry<String, String> entry : tags.entrySet()) {
//                builder.append(",");
//                builder.append(entry.getKey());
//                builder.append("=");
//                builder.append(doWithTag(entry.getValue()));
//            }
//        }
//
//        builder.append(" ");
//        Map<String, Object> values = metricData.getValues();
//        if (!values.isEmpty()) {
//            for (Map.Entry<String, Object> entry : values.entrySet()) {
//                builder.append(entry.getKey());
//                builder.append("=");
//                Object fieldV = entry.getValue();
//                if (fieldV instanceof String) {
//                    builder.append("\"");
//                    builder.append(doWithValue((String) (entry.getValue())));
//                    builder.append("\"");
//                } else {
//                    builder.append(entry.getValue());
//                }
//                builder.append(",");
//            }
//            builder.deleteCharAt(builder.length() - 1);
//            builder.append(" ");
//        }
//        builder.append(TimeUnit.NANOSECONDS.convert(metricData.getTimestamp(), TimeUnit.MILLISECONDS));
//        if (ApolloUtils.isDebugMode() && builder.toString().contains("exception_stack")) {
//            System.out.println(builder.toString());
//        }
//        return builder.toString();
//    }
//
//    private static String doWithTag(String tag) {
//        if (tag == null || tag.trim().equals("")) {
//            tag = " ";
//        }
//        StringBuffer sb = new StringBuffer();
//        if (tag != null) {
//            for (int i = 0; i < tag.length(); i++) {
//                char c = tag.charAt(i);
//                switch (c) {
//                    case ',':
//                        sb.append("\\,");
//                        break;
//                    case '\n':
//                        sb.append("\\n");
//                        break;
//                    case '=':
//                        sb.append("\\=");
//                        break;
//                    case ' ':
//                        sb.append("\\ ");
//                        break;
//                    default:
//                        sb.append(c);
//                }
//            }
//        }
//        return sb.toString();
//    }
//
//    private static String doWithValue(String value) {
//        StringBuffer sb = new StringBuffer();
//        if (value != null) {
//            for (int i = 0; i < value.length(); i++) {
//                char c = value.charAt(i);
//                switch (c) {
//                    case '"':
//                        sb.append("\\\"");
//                        break;
//                    default:
//                        sb.append(c);
//                }
//            }
//        }
//        return sb.toString();
//    }
//}