package com.resharding.db;

import com.resharding.config.ReshardingRootProperties;
import org.springframework.stereotype.Component;

import java.time.YearMonth;
import java.time.format.DateTimeFormatter;

/**
 * Разрешает имя физической партиции PostgreSQL по шаблону из конфигурации.
 *
 * <p>Пример: {@code orders_2024_01} для {@code namePattern: "{table}_{yyyy}_{MM}"}.
 */
@Component
public class PartitionNameResolver {

    /**
     * Возвращает имя физической партиции для календарного месяца.
     *
     * @param parentTable имя родительской таблицы
     * @param yearMonth   календарный месяц партиции
     * @param partition   конфигурация партиционирования
     * @return имя child-партиции в PostgreSQL
     */
    public String resolve(String parentTable, YearMonth yearMonth, ReshardingRootProperties.PartitionConfig partition) {
        String pattern = partition.getNamePattern();
        if (pattern == null || pattern.isBlank()) {
            pattern = "{table}_{yyyy}_{MM}";
        }
        return pattern
                .replace("{table}", parentTable)
                .replace("{yyyy}", yearMonth.format(DateTimeFormatter.ofPattern("yyyy")))
                .replace("{MM}", yearMonth.format(DateTimeFormatter.ofPattern("MM")));
    }

    /**
     * Ключ партиции для etcd ({@code 2024-01}).
     */
    public String partitionKey(YearMonth yearMonth) {
        return yearMonth.toString();
    }
}
