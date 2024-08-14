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

import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.constants.DataSourceConstants;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.AbstractDataSourceProcessor;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.PasswordUtils;
import org.apache.dolphinscheduler.spi.datasource.BaseConnectionParam;
import org.apache.dolphinscheduler.spi.datasource.ConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;

import org.apache.commons.collections4.MapUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.google.auto.service.AutoService;

@AutoService(DataSourceProcessor.class)
public class SybaseDataSourceProcessor extends AbstractDataSourceProcessor {

    @Override
    public BaseDataSourceParamDTO castDatasourceParamDTO(String paramJson) {
        return JSONUtils.parseObject(paramJson, SybaseDataSourceParamDTO.class);
    }

    @Override
    public BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson) {
        SybaseConnectionParam connectionParams = (SybaseConnectionParam) createConnectionParams(connectionJson);
        String[] hostSeperator = connectionParams.getAddress().split(Constants.DOUBLE_SLASH);
        String[] hostPortArray = hostSeperator[hostSeperator.length - 1].split(Constants.COMMA);

        SybaseDataSourceParamDTO sybaseDatasourceParamDTO = new SybaseDataSourceParamDTO();
        sybaseDatasourceParamDTO.setDatabase(connectionParams.getDatabase());
        sybaseDatasourceParamDTO.setUserName(connectionParams.getUser());
        sybaseDatasourceParamDTO.setOther(connectionParams.getOther());
        sybaseDatasourceParamDTO.setPort(Integer.parseInt(hostPortArray[0].split(Constants.COLON)[1]));
        sybaseDatasourceParamDTO.setHost(hostPortArray[0].split(Constants.COLON)[0]);
        return sybaseDatasourceParamDTO;
    }

    @Override
    public BaseConnectionParam createConnectionParams(BaseDataSourceParamDTO datasourceParam) {
        SybaseDataSourceParamDTO sybaseParam = (SybaseDataSourceParamDTO) datasourceParam;
        String address =
                String.format("%s%s:%s", DataSourceConstants.JDBC_SYBASE, sybaseParam.getHost(),
                        sybaseParam.getPort());
        String jdbcUrl = address + "/" + sybaseParam.getDatabase();

        SybaseConnectionParam sybaseConnectionParam = new SybaseConnectionParam();
        sybaseConnectionParam.setAddress(address);
        sybaseConnectionParam.setDatabase(sybaseParam.getDatabase());
        sybaseConnectionParam.setJdbcUrl(jdbcUrl);
        sybaseConnectionParam.setOther(sybaseParam.getOther());
        sybaseConnectionParam.setUser(sybaseParam.getUserName());
        sybaseConnectionParam.setPassword(PasswordUtils.encodePassword(sybaseParam.getPassword()));
        sybaseConnectionParam.setDriverClassName(getDatasourceDriver());
        sybaseConnectionParam.setValidationQuery(getValidationQuery());
        return sybaseConnectionParam;
    }

    @Override
    public BaseConnectionParam createConnectionParams(String connectionJson) {
        return JSONUtils.parseObject(connectionJson, SybaseConnectionParam.class);
    }

    @Override
    public String getDatasourceDriver() {
        return DataSourceConstants.COM_SYBASE_JDBC_DRIVER;
    }

    @Override
    public String getValidationQuery() {
        return DataSourceConstants.SYBASE_VALIDATION_QUERY;
    }

    @Override
    public String getJdbcUrl(ConnectionParam connectionParam) {
        SybaseConnectionParam sybaseConnectionParam = (SybaseConnectionParam) connectionParam;

        if (MapUtils.isNotEmpty(sybaseConnectionParam.getOther())) {
            return String.format("%s;%s", sybaseConnectionParam.getJdbcUrl(),
                    transformOther(sybaseConnectionParam.getOther()));
        }
        return sybaseConnectionParam.getJdbcUrl();
    }

    @Override
    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        SybaseConnectionParam sybaseConnectionParam = (SybaseConnectionParam) connectionParam;
        Class.forName(getDatasourceDriver());
        String jdbcUrl = getJdbcUrl(connectionParam);
        String user = sybaseConnectionParam.getUser();
        String password = PasswordUtils.decodePassword(sybaseConnectionParam.getPassword());
        return DriverManager.getConnection(jdbcUrl, user, password);
    }

    @Override
    public DbType getDbType() {
        return DbType.SYBASE;
    }

    @Override
    public DataSourceProcessor create() {
        return new SybaseDataSourceProcessor();
    }

    @Override
    public List<String> splitAndRemoveComment(String sql) {
        return SQLParserUtils.splitAndRemoveComment(sql, com.alibaba.druid.DbType.sybase)
                .stream()
                .map(subSql -> subSql.concat(";"))
                .collect(Collectors.toList());
    }

    private String transformOther(Map<String, String> otherMap) {
        if (MapUtils.isEmpty(otherMap)) {
            return null;
        }
        StringBuilder stringBuilder = new StringBuilder();
        otherMap.forEach((key, value) -> stringBuilder.append(String.format("%s=%s;", key, value)));
        return stringBuilder.toString();
    }

}
