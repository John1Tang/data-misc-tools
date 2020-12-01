package com.thenetcircle.service.data.hive.udf.commons;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @description: RegexUtil
 * @author: John Tang
 * @create: 2018-10-16 17:37
 **/
public class RegexUtil {

    static final Logger log = LoggerFactory.getLogger(RegexUtil.class.getName());
    private String lastRegex = null;
    private Pattern p = null;

    private RegexUtil() {
    }

    private static class RegexToolHolder {
        private static final RegexUtil INSTANCE = new RegexUtil();
    }

    public static RegexUtil getRegex (){
        return RegexUtil.RegexToolHolder.INSTANCE;
    }


    /**
     * one match
     * @param strInput
     * @param expre
     * @return
     */
    public String getForCustom(String strInput, String expre) {
        if (!StringUtils.isNotEmpty(strInput) || !StringUtils.isNotEmpty(expre)) {
            log.error("input :" + strInput + "\n regex:" + expre);
            return "";
        }

        if (!expre.equals(lastRegex) || p == null){
            lastRegex = expre;
            p = Pattern.compile(expre);
        }

        Matcher mMatcher = p.matcher(strInput);
        int groupCount = mMatcher.groupCount();
        String val = "";
        int groupIndex = groupCount > 0 ? 1 : 0;
        for (; groupIndex <= groupCount; groupIndex++) {
            while (mMatcher.find()) {
                String group = mMatcher.group(groupIndex);
                if (StringUtils.isNotEmpty(group)) {
                    val = group;
                    break;
                }
            }
            //reset if not matched
            if (!StringUtils.isNotEmpty(val)) {
                mMatcher.reset();
            } else {
                break;
            }
        }
        return !StringUtils.isNotEmpty(val) ? "" : val;
    }

    /**
     * multi match
     * @param strInput
     * @param expre
     * @return
     */
    public List<String> multiMatchResults(String strInput, String expre) {
        if (!StringUtils.isNotEmpty(strInput) || !StringUtils.isNotEmpty(expre)) {
            log.error("input :" + strInput + "\n regex:" + expre);
            return Collections.emptyList();
        }

        if (!expre.equals(lastRegex) || p == null){
            lastRegex = expre;
            p = Pattern.compile(expre);
        }

        List<String> listVal = new ArrayList<>();
        Matcher mMatcher = p.matcher(strInput);
        int groupCount = mMatcher.groupCount();
        int groupIndex = groupCount > 0 ? 1 : 0;
        for (; groupIndex <= groupCount; groupIndex++) {
            while (mMatcher.find()) {
                String group = mMatcher.group(groupIndex);
                if (StringUtils.isNotEmpty(group)) {
                    listVal.add(group);
                }
            }
            //reset if not matched
            if (CollectionUtils.isEmpty(listVal)) {
                mMatcher.reset();
            } else {
                break;
            }
        }
        return CollectionUtils.isEmpty(listVal) ? Collections.emptyList() : listVal;
    }
}
