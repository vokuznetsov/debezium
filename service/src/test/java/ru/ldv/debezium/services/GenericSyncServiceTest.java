package ru.ldv.debezium.services;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayName("Юнит-тесты динамического сервиса синхронизации GenericSyncService")
class GenericSyncServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    private GenericSyncService syncService;

    @BeforeEach
    void setUp() {
        syncService = new GenericSyncService(jdbcTemplate);
    }

    @Test
    @DisplayName("Генерация корректного SQL MERGE (UPSERT) запроса и передача параметров в JdbcTemplate")
    void upsertCorrectMergeSql() {
        Map<String, Object> rowData = new LinkedHashMap<>();
        rowData.put("id", 1L);
        rowData.put("name", "John Doe");

        syncService.upsert("dbo", "users", rowData, List.of("id"));

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sqlCaptor.capture(), (Object[]) any());

        String sql = sqlCaptor.getValue();
        assertThat(sql).contains("MERGE INTO [dbo].[users] AS target");
        assertThat(sql).contains("USING (SELECT ? AS [id], ? AS [name]) AS source");
        assertThat(sql).contains("ON target.[id] = source.[id]");
        assertThat(sql).contains("WHEN MATCHED THEN UPDATE SET target.[name] = source.[name]");
        assertThat(sql).contains("WHEN NOT MATCHED THEN INSERT ([id], [name]) VALUES (source.[id], source.[name])");
    }

    @Test
    @DisplayName("Генерация корректного DELETE запроса по первичному ключу")
    void deleteCorrectDeleteSql() {
        Map<String, Object> rowData = Map.of("id", 42L);

        syncService.delete("dbo", "users", rowData, List.of("id"));

        verify(jdbcTemplate).update(eq("DELETE FROM [dbo].[users] WHERE [id] = ?"), eq(42L));
    }
}