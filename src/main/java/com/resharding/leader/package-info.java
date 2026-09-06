/**
 * Логика Leader: планирование Task, sliding window, checkpoint, incremental cycle.
 *
 * <p>Только Leader управляет текущей migration. Worker не создаёт Task самостоятельно.
 */
package com.resharding.leader;
