package com.resharding.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Информация о текущем Leader кластера.
 *
 * <p>Ключ {@code {etcd.prefix}leader} существует под lease.
 * Записывается через atomic {@code put-if-absent}; продлевается keepalive.
 * Старый Leader после истечения lease теряет право управлять migration (fencing).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LeaderInfo {

    /** {@code instanceId} Pod, избранного Leader'ом. */
    private String instanceId;

    /** Номер срока (term) лидерства; увеличивается при каждой новой кампании. */
    private long term;

    /** Время избрания Leader'ом. */
    private LocalDateTime electedAt;
}
