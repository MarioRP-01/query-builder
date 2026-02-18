package com.enterprise.batch.shared.tasklets.adapter;

import com.enterprise.batch.shared.tasklets.port.BusinessDateResolver;
import com.enterprise.batch.shared.tasklets.port.HolidayCalendar;

import java.time.DayOfWeek;
import java.time.LocalDate;

/**
 * Default implementation of {@link BusinessDateResolver}.
 * Walks backwards from today, skipping weekends (and holidays for business days).
 */
public class DefaultBusinessDateResolver implements BusinessDateResolver {

    private final HolidayCalendar calendar;

    public DefaultBusinessDateResolver(HolidayCalendar calendar) {
        this.calendar = calendar;
    }

    @Override
    public LocalDate lastWeekday() {
        LocalDate date = LocalDate.now().minusDays(1);
        while (isWeekend(date)) {
            date = date.minusDays(1);
        }
        return date;
    }

    @Override
    public LocalDate lastBusinessDay() {
        LocalDate date = LocalDate.now().minusDays(1);
        while (isWeekend(date) || calendar.isHoliday(date)) {
            date = date.minusDays(1);
        }
        return date;
    }

    private static boolean isWeekend(LocalDate date) {
        DayOfWeek day = date.getDayOfWeek();
        return day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY;
    }
}
