package com.resharding.db;

import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Определяет, завершён ли календарный месяц по продвижению checkpoint.
 *
 * <p>Месяц считается полностью залитым, когда {@code lastProcessedCreatedAt}
 * checkpoint перешёл в следующий календарный месяц.
 */
@Component
public class PartitionCompletionDetector {

    /**
     * Возвращает месяц, который только что был полностью перенесён.
     *
     * @param previousCheckpoint предыдущее значение {@code lastProcessedCreatedAt}
     * @param newCheckpoint      новое значение после завершения Task
     */
    public Optional<YearMonth> detectCompletedMonth(LocalDateTime previousCheckpoint, LocalDateTime newCheckpoint) {
        return detectCompletedMonths(previousCheckpoint, newCheckpoint).stream().findFirst();
    }

    /**
     * Возвращает все месяцы, границы которых пересёк checkpoint.
     * Это важно при range-duration больше месяца: прежний метод возвращал
     * только первый месяц, и промежуточные партиции оставались без индексов.
     */
    public List<YearMonth> detectCompletedMonths(
            LocalDateTime previousCheckpoint, LocalDateTime newCheckpoint) {
        if (newCheckpoint == null) {
            return List.of();
        }

        YearMonth newMonth = YearMonth.from(newCheckpoint);
        if (previousCheckpoint == null) {
            return List.of();
        }

        YearMonth previousMonth = YearMonth.from(previousCheckpoint);
        List<YearMonth> completed = new ArrayList<>();
        for (YearMonth month = previousMonth; month.isBefore(newMonth); month = month.plusMonths(1)) {
            completed.add(month);
        }
        return completed;
    }
}
