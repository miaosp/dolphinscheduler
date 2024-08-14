/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.plugin.datasource.sybase.param;

import static com.google.common.truth.Truth.assertThat;

import org.apache.dolphinscheduler.common.constants.DataSourceConstants;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.PasswordUtils;
import org.apache.dolphinscheduler.spi.enums.DbType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SybaseDataSourceProcessorTest {

    private SybaseDataSourceProcessor sybaseDatasourceProcessor = new SybaseDataSourceProcessor();

    @Test
    public void testCreateConnectionParams() {
        Map<String, String> props = new HashMap<>();
        props.put("serverTimezone", "utc");
        SybaseDataSourceParamDTO sybaseDatasourceParamDTO = new SybaseDataSourceParamDTO();
        sybaseDatasourceParamDTO.setUserName("root");
        sybaseDatasourceParamDTO.setPassword("123456");
        sybaseDatasourceParamDTO.setDatabase("default");
        sybaseDatasourceParamDTO.setHost("localhost");
        sybaseDatasourceParamDTO.setPort(1234);
        sybaseDatasourceParamDTO.setOther(props);

        try (MockedStatic<PasswordUtils> mockedStaticPasswordUtils = Mockito.mockStatic(PasswordUtils.class)) {
            mockedStaticPasswordUtils.when(() -> PasswordUtils.encodePassword(Mockito.anyString())).thenReturn("test");
            SybaseConnectionParam connectionParams = (SybaseConnectionParam) sybaseDatasourceProcessor
                    .createConnectionParams(sybaseDatasourceParamDTO);
            Assertions.assertEquals("jdbc:sybase:Tds:localhost:1234", connectionParams.getAddress());
            Assertions.assertEquals("jdbc:sybase:Tds:localhost:1234/default",
                    connectionParams.getJdbcUrl());
            Assertions.assertEquals("root", connectionParams.getUser());
        }
    }

    @Test
    public void testCreateConnectionParams2() {
        String connectionJson =
                "{\"user\":\"root\",\"password\":\"123456\",\"address\":\"jdbc:sybase:Tds:localhost:1234/default"
                        + "\",\"jdbcUrl\":\"jdbc:sybase:Tds:localhost:1234/default\"}";
        SybaseConnectionParam sybaseConnectionParam =
                JSONUtils.parseObject(connectionJson, SybaseConnectionParam.class);
        Assertions.assertNotNull(sybaseConnectionParam);
        Assertions.assertEquals("root", sybaseConnectionParam.getUser());
    }

    @Test
    public void testGetDatasourceDriver() {
        Assertions.assertEquals(DataSourceConstants.COM_SYBASE_JDBC_DRIVER,
                sybaseDatasourceProcessor.getDatasourceDriver());
    }

    @Test
    public void testGetJdbcUrl() {
        SybaseConnectionParam sybaseConnectionParam = new SybaseConnectionParam();
        sybaseConnectionParam.setJdbcUrl("jdbc:sybase:Tds:localhost:1234/default");
        Assertions.assertEquals("jdbc:sybase:Tds:localhost:1234/default",
                sybaseDatasourceProcessor.getJdbcUrl(sybaseConnectionParam));
    }

    @Test
    public void testGetDbType() {
        Assertions.assertEquals(DbType.SYBASE, sybaseDatasourceProcessor.getDbType());
    }

    @Test
    public void testGetValidationQuery() {
        Assertions.assertEquals(DataSourceConstants.SYBASE_VALIDATION_QUERY,
                sybaseDatasourceProcessor.getValidationQuery());
    }

    @Test
    void splitAndRemoveComment_singleSelect() {
        String sql = "select * from table;";
        List<String> subSqls = sybaseDatasourceProcessor.splitAndRemoveComment(sql);
        assertThat(subSqls).hasSize(1);
        assertThat(subSqls.get(0)).isEqualTo("select * from table;");
    }

    @Test
    void splitAndRemoveComment_singleMerge() {
        String sql = "MERGE\n" +
                "    [ TOP ( expression ) [ PERCENT ] ]\n" +
                "    [ INTO ] <target_table> [ WITH ( <merge_hint> ) ] [ [ AS ] table_alias ]\n" +
                "    USING <table_source> [ [ AS ] table_alias ]\n" +
                "    ON <merge_search_condition>\n" +
                "    [ WHEN MATCHED [ AND <clause_search_condition> ]\n" +
                "        THEN <merge_matched> ] [ ...n ]\n" +
                "    [ WHEN NOT MATCHED [ BY TARGET ] [ AND <clause_search_condition> ]\n" +
                "        THEN <merge_not_matched> ]\n" +
                "    [ WHEN NOT MATCHED BY SOURCE [ AND <clause_search_condition> ]\n" +
                "        THEN <merge_matched> ] [ ...n ]\n" +
                "    [ <output_clause> ]\n" +
                "    [ OPTION ( <query_hint> [ ,...n ] ) ];";
        List<String> subSqls = sybaseDatasourceProcessor.splitAndRemoveComment(sql);
        assertThat(subSqls).hasSize(1);
        assertThat(subSqls.get(0)).isEqualTo(sql);
    }
}
