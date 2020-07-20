package com.thenetcircle.service.data.hive.udf.commons;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.List;

/**
 * @description:
 * @author: john
 * @created: 2020/05/20 16:54
 */
@Description(name = "reg_multi_match",
        value = "_FUNC_(input, regexp) - list all results match with the given regular expression")
public class UDFRegMultiMatch extends UDF {

    public UDFRegMultiMatch(){}

    public String evaluate(String input, String regex){
        List<String> multiMatchResults = RegexUtil.getRegex().multiMatchResults(input, regex);
        return '['+ StringUtils.join(multiMatchResults, ',')+ ']';
    }

}
