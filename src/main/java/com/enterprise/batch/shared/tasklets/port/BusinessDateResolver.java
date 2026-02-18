package com.enterprise.batch.shared.tasklets.port;

import java.time.LocalDate;

/**
 * Resolves business-relevant dates for a batch run.
 */
public interface BusinessDateResolver {

    /**
     * Previous day that falls on Monday–Friday (skips weekends only).
     */
    LocalDate lastWeekday();

    /**
     * Previous day that falls on Monday–Friday and is not a holiday.
     */
    LocalDate lastBusinessDay();
}
