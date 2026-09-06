package com.resharding.db;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Проверяет преобразование движения checkpoint в список завершённых месяцев.
 *
 * <p>Индекс партиции разрешено создавать только после прохождения границы месяца.
 * Отдельный тест большого скачка нужен, чтобы не потерять промежуточные месяцы
 * при увеличенном размере Task range.
 */
class PartitionCompletionDetectorTest {

    private final PartitionCompletionDetector detector = new PartitionCompletionDetector();

    @Test
    void detectsCompletedMonthWhenCheckpointCrossesMonthBoundary() {
        LocalDateTime before = LocalDateTime.of(2024, 1, 31, 23, 0);
        LocalDateTime after = LocalDateTime.of(2024, 2, 1, 0, 0);

        Optional<YearMonth> completed = detector.detectCompletedMonth(before, after);

        assertThat(completed).contains(YearMonth.of(2024, 1));
    }

    @Test
    void noCompletedMonthWithinSameMonth() {
        LocalDateTime before = LocalDateTime.of(2024, 1, 10, 0, 0);
        LocalDateTime after = LocalDateTime.of(2024, 1, 20, 0, 0);

        Optional<YearMonth> completed = detector.detectCompletedMonth(before, after);

        assertThat(completed).isEmpty();
    }

    @Test
    void noCompletedMonthWhenPreviousCheckpointMissing() {
        LocalDateTime after = LocalDateTime.of(2024, 2, 1, 0, 0);

        Optional<YearMonth> completed = detector.detectCompletedMonth(null, after);

        assertThat(completed).isEmpty();
    }

    @Test
    void returnsEveryMonthCrossedByLargeRange() {
        assertThat(detector.detectCompletedMonths(
                LocalDateTime.of(2024, 1, 15, 0, 0),
                LocalDateTime.of(2024, 4, 1, 0, 0)))
                .containsExactly(
                        YearMonth.of(2024, 1),
                        YearMonth.of(2024, 2),
                        YearMonth.of(2024, 3));
    }
}
