package ru.ldv.debezium.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class GenericSyncService {

    private final JdbcTemplate jdbcTemplate;

    /**
     * Динамический MERGE (UPSERT) с учетом схемы MS SQL
     */
    @Transactional
    public void upsert(String schemaName, String tableName, Map<String, Object> rowData, List<String> primaryKeys) {
        if (rowData == null || rowData.isEmpty()) {
            log.warn("Skipping MERGE: rowData is null or empty for table {}.{}", schemaName, tableName);
            return;
        }

        List<String> columns = new ArrayList<>(rowData.keySet());

        // Блок USING (SELECT ? AS [col1], ? AS [col2])
        String sourceSelect = columns.stream()
                .map(col -> "? AS [" + col + "]")
                .collect(Collectors.joining(", "));

        // Условие соединения ON (target.[pk1] = source.[pk1] AND target.[pk2] = source.[pk2])
        String joinCondition = primaryKeys.stream()
                .map(pk -> String.format("target.[%s] = source.[%s]", pk, pk))
                .collect(Collectors.joining(" AND "));

        // Блок UPDATE SET target.[col] = source.[col]
        String updateSet = columns.stream()
                .filter(col -> primaryKeys.stream().noneMatch(pk -> pk.equalsIgnoreCase(col)))
                .map(col -> "target.[" + col + "] = source.[" + col + "]")
                .collect(Collectors.joining(", "));

        // Блок INSERT ([col1], [col2]) VALUES (source.[col1], source.[col2])
        String insertCols = columns.stream().map(col -> "[" + col + "]").collect(Collectors.joining(", "));
        String insertVals = columns.stream().map(col -> "source.[" + col + "]").collect(Collectors.joining(", "));

        // Полные имена с учетом схемы [schema].[table]
        String fullTableName = String.format("[%s].[%s]", schemaName, tableName);

        // В случае, если таблица состоит ТОЛЬКО из первичных ключей (без дополнительных колонок для UPDATE)
        String matchedClause = updateSet.isEmpty() ? "" : "WHEN MATCHED THEN UPDATE SET " + updateSet;

        String sql = String.format("""
            SET IDENTITY_INSERT %s ON;
            MERGE INTO %s AS target
            USING (SELECT %s) AS source ON %s
            %s
            WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);
            SET IDENTITY_INSERT %s OFF;
            """, fullTableName, fullTableName, sourceSelect, joinCondition, matchedClause, insertCols, insertVals, fullTableName);

        Object[] params = columns.stream()
                .map(rowData::get)
                .toArray();

        jdbcTemplate.update(sql, params);
    }

    /**
     * Динамический DELETE с учетом схемы MS SQL
     */
    @Transactional
    public void delete(String schemaName, String tableName, Map<String, Object> rowData, List<String> primaryKeys) {
        if (rowData == null || rowData.isEmpty()) return;

        // Динамическое условие WHERE [pk1] = ? AND [pk2] = ?
        String whereCondition = primaryKeys.stream()
                .map(pk -> String.format("[%s] = ?", pk))
                .collect(Collectors.joining(" AND "));

        String fullTableName = String.format("[%s].[%s]", schemaName, tableName);
        String sql = String.format("DELETE FROM %s WHERE %s", fullTableName, whereCondition);

        Object[] pkValues = primaryKeys.stream()
                .map(rowData::get)
                .toArray();

        jdbcTemplate.update(sql, pkValues);
    }
}
