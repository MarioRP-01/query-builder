package com.enterprise.batch.shared.tasklets.port;

import java.time.LocalDate;

/**
 * Determines whether a given date is a holiday.
 */
@FunctionalInterface
public interface HolidayCalendar {

    boolean isHoliday(LocalDate date);
}
