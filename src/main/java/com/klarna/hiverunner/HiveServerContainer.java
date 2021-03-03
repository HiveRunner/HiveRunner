/**
 * Copyright (C) 2013-2021 Klarna AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.klarna.hiverunner;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.klarna.hiverunner.builder.Statement;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveVariableSource;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.ql.exec.tez.TezJobExecHelper;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.Service;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.server.HiveServer2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HiveServer wrapper
 */
public class HiveServerContainer {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveServerContainer.class);

    private CLIService client;
    private final HiveServerContext context;
    private SessionHandle sessionHandle;
    private HiveServer2 hiveServer2;
    private SessionState currentSessionState;

    public HiveServerContainer(HiveServerContext context) {
        this.context = context;
    }

    public CLIService getClient() {
        return client;
    }

    /**
     * Will start the HiveServer.
     *
     * @param testConfig Specific test case properties. Will be merged with the HiveConf of the context
     * @param hiveVars   HiveVars to pass on to the HiveServer for this session
     */
    public void init(Map<String, String> testConfig, Map<String, String> hiveVars) {

        context.init();

        HiveConf hiveConf = context.getHiveConf();

        // merge test case properties with hive conf before HiveServer is started.
        for (Map.Entry<String, String> property : testConfig.entrySet()) {
            hiveConf.set(property.getKey(), property.getValue());
        }

        try {
            hiveServer2 = new HiveServer2();
            hiveServer2.init(hiveConf);

            // Locate the ClIService in the HiveServer2
            for (Service service : hiveServer2.getServices()) {
                if (service instanceof CLIService) {
                    client = (CLIService) service;
                }
            }

            Preconditions.checkNotNull(client, "ClIService was not initialized by HiveServer2");

            sessionHandle = client.openSession("noUser", "noPassword", null);

            SessionState sessionState = client.getSessionManager().getSession(sessionHandle).getSessionState();
            currentSessionState = sessionState;
            currentSessionState.setHiveVariables(hiveVars);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create HiveServer :" + e.getMessage(), e);
        }

        // Ping hive server before we do anything more with it! If validation
        // is switched on, this will fail if metastorage is not set up properly
        pingHiveServer();
    }

    public Path getBaseDir() {
        return context.getBaseDir();
    }

    public List<Object[]> executeStatement(Statement hiveql) {
        return executeStatement(hiveql.getSql());
    }

    public List<Object[]> executeStatement(String hiveql) {
        try {
            if(isViewJoin(hiveql)) {
                hiveql = transformQuery3(hiveql);
              //hiveql =  transformQuery2(hiveql);
              //int index = hiveql.toLowerCase().indexOf(" on ");
              //hiveql = hiveql.substring(0, index) + hiveql.substring(index, hiveql.length()).toLowerCase() ;
              System.out.println("hiveqtransformed:"+hiveql);
            }
            OperationHandle handle = client.executeStatement(sessionHandle, hiveql, new HashMap<>());
            System.out.println("executes hiveql");
            List<Object[]> resultSet = new ArrayList<>();
            if (handle.hasResultSet()) {

                /*
                 * fetchResults will by default return 100 rows per fetch (hive 14). For big result sets we need to continuously fetch the result set until all
                 * rows are fetched.
                 */
                RowSet rowSet;
                while ((rowSet = client.fetchResults(handle)) != null && rowSet.numRows() > 0) {
                    for (Object[] row : rowSet) {
                        resultSet.add(row.clone());
                    }
                }
            }

            LOGGER.debug("ResultSet:\n"
                    + Joiner.on("\n").join(Iterables.transform(resultSet, new Function<Object[], String>() {
                        @Nullable
                        @Override
                        public String apply(@Nullable Object[] objects) {
                            return Joiner.on(", ").useForNull("null").join(objects);
                        }
                    })));

            return resultSet;
        } catch (HiveSQLException e) {
            throw new IllegalArgumentException("Failed to executeQuery Hive query " + hiveql + ": " + e.getMessage(),
                    e);
        }
    }

    public String transformQuery3(String hiveql) {
        String result = hiveql;
        if(hiveql.contains("'")){
            //First we leave the query untouched from whatever is before the on
            int indexOn = hiveql.toLowerCase().indexOf(" on ");
            result = hiveql.substring(0, indexOn);

            System.out.println("initial result1:"+result);
            int indexFirstComa = hiveql.indexOf("'");
            //result = result + hiveql.substring(indexOn, indexFirstComa).toLowerCase();

            //System.out.println("initial result2:"+result);

            String subString = hiveql.substring(indexOn, hiveql.length());
            int count = StringUtils.countMatches(subString, "'");
            System.out.println("count:"+count);
            int indexStart = subString.indexOf("'");

            for(int i = 0; i < count/2; i++) {
                indexStart = subString.indexOf("'");
                result = result + subString.substring(0,indexStart).toLowerCase();


                System.out.println("result3:"+result);
                System.out.println("substring:"+subString);
                //subString = hiveql.substring(index, hiveql.length());

                //result = result + result.substring(indexStart,)
                System.out.println("index of first ':"+indexStart);
                String caseSensitiveString = subString.substring(indexStart+1, subString.length());
                System.out.println("sensitive string1:"+caseSensitiveString);
                int indexEnd = caseSensitiveString.indexOf("'");
                System.out.println("indexEnd:"+indexEnd);
                caseSensitiveString = caseSensitiveString.substring(0, indexEnd);
                System.out.println("sensitive string2:"+caseSensitiveString);
                result = result +"'"+ caseSensitiveString +"'";
                System.out.println("result4:"+result);
                subString = subString.substring(indexStart+caseSensitiveString.length()+2,subString.length());
                System.out.println("new substring:"+subString);
                indexStart = indexEnd;

            }
            result = result + hiveql.substring(result.length(),hiveql.length());
        }
        return result;
    }
    public String transformQuery2(String hiveql){
        //System.out.println("before transformation:"+hiveql);
        int indexOn = hiveql.toLowerCase().indexOf(" on ");
        String subString = hiveql.substring(indexOn, hiveql.length());
        int indexEqual = subString.indexOf("=");

        //delete all whitespace after equal
        while(String.valueOf(subString.charAt(indexEqual+1)).equals(" ")){
            StringBuilder sb = new StringBuilder(subString);
            sb.deleteCharAt(indexEqual+1);
            subString = sb.toString();
        }
        //delete all whitespace before equal
        while(String.valueOf(subString.charAt(indexEqual-1)).equals(" ")){
            StringBuilder sb = new StringBuilder(subString);
            sb.deleteCharAt(indexEqual-1);
            subString = sb.toString();
            indexEqual = subString.indexOf("=");
        }
        //now we find the indexes, find index of first whitespace AFTER equal
        int indexEnd = indexEqual;
        while(!String.valueOf(subString.charAt(indexEnd+1)).equals(" ")){
            indexEnd++;
        }

        //Find index of first whitespace BEFORE equal
        int indexStart = indexEqual;
        while(!String.valueOf(subString.charAt(indexStart-1)).equals(" ")){
            indexStart--;
        }

        //Now we can turn to lowercase exactly what we want
        subString = subString.substring(0, indexStart) + subString.substring(indexStart, indexEnd).toLowerCase() + subString.substring(indexEnd, subString.length());
        hiveql =  hiveql.substring(0, indexOn) + subString;
        //System.out.println("after transformation:"+hiveql);
        return hiveql;
    }

    public String transformQuery(String hiveql){
        int indexOn = hiveql.toLowerCase().indexOf(" on ");
        String subQuery = hiveql.substring(indexOn, hiveql.length()).toLowerCase();

        int indexWhere = subQuery.toLowerCase().indexOf(" where ");
        System.out.println("index where:"+indexWhere);
        int indexAnd= subQuery.toLowerCase().indexOf(" and ");
        System.out.println("index and :"+indexAnd);
        int indexOr= subQuery.toLowerCase().indexOf(" or ");
        System.out.println("index or :"+indexOr);
        List<Integer> indexes = null;
        indexes.add(indexWhere);
        indexes.add(indexOr);
        indexes.add(indexAnd);
        return "LOL";
    }

    public boolean isViewJoin(String hiveql){
        hiveql = hiveql.toLowerCase();
        return (hiveql.contains(" view ")
            && hiveql.contains(" join ")
            && hiveql.contains(" on "));
    }

    /**
     * Release all resources.
     * <p>
     * This call will never throw an exception as it makes no sense doing that in the tear down phase.
     * </p>
     */
    public void tearDown() {

        try {
            TezJobExecHelper.killRunningJobs();
        } catch (Throwable e) {
            LOGGER.warn("Failed to kill tez session: " + e.getMessage() + ". Turn on log level debug for stacktrace");
            LOGGER.debug(e.getMessage(), e);
        }

        try {
            // Reset to default schema
            executeStatement("USE default");
        } catch (Throwable e) {
            LOGGER.warn("Failed to reset to default schema: " + e.getMessage()
                    + ". Turn on log level debug for stacktrace");
            LOGGER.debug(e.getMessage(), e);
        }

        try {
            client.closeSession(sessionHandle);
        } catch (Throwable e) {
            LOGGER.warn(
                "Failed to close client session: " + e.getMessage() + ". Turn on log level debug for stacktrace");
            LOGGER.debug(e.getMessage(), e);
        }

        try {
            hiveServer2.stop();
        } catch (Throwable e) {
            LOGGER.warn("Failed to stop HiveServer2: " + e.getMessage() + ". Turn on log level debug for stacktrace");
            LOGGER.debug(e.getMessage(), e);
        }

        hiveServer2 = null;
        client = null;
        sessionHandle = null;

        LOGGER.info("Tore down HiveServer instance");
    }

    public String expandVariableSubstitutes(String expression) {
        return getVariableSubstitution().substitute(getHiveConf(), expression);
    }

    private void pingHiveServer() {
        executeStatement("SHOW TABLES");
    }

    public HiveConf getHiveConf() {
        return hiveServer2.getHiveConf();
    }

    public VariableSubstitution getVariableSubstitution() {
        // Make sure to set the session state for this thread before returning the VariableSubstitution. If not set,
        // hivevar:s will not be evaluated.
        SessionState.setCurrentSessionState(currentSessionState);

        SessionState ss = currentSessionState;
        return new VariableSubstitution(new HiveVariableSource() {
            @Override
            public Map<String, String> getHiveVariable() {
                return ss.getHiveVariables();
            }
        });
    }
}
