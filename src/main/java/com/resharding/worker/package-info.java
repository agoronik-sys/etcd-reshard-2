/**
 * Worker: захват Task, batch-обработка, fencing через {@code generation}.
 *
 * <p>Инвариант: один Pod выполняет не более одной migration Task одновременно.
 */
package com.resharding.worker;
