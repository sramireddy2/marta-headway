package dev.headway.gtfs;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Which scheduled services run on a given date.
 *
 * <p>A GTFS feed contains every trip for every kind of day at once — MARTA's holds 52,401 of them.
 * Weekday, Saturday, Sunday and holiday trips all sit in the same {@code trips.txt}, distinguished
 * only by {@code service_id}. Asking "how often is route 15 scheduled right now?" without first
 * filtering to today's services would average a Friday rush hour together with a Sunday morning
 * and report a headway roughly half the real one, which would then make every bus look bunched.
 *
 * <h2>Two files, and the exceptions win</h2>
 *
 * <ul>
 *   <li>{@code calendar.txt} gives the regular pattern: service 5 runs Monday to Friday, 3 runs
 *       Saturday, 4 runs Sunday, and a dozen others have every day set to 0.
 *   <li>{@code calendar_dates.txt} overrides it for specific dates. MARTA's has eight rows, all in
 *       pairs: on 2026-07-04, service 29 is <em>added</em> ({@code exception_type} 1) and service
 *       3 is <em>removed</em> ({@code exception_type} 2). That is the holiday schedule, and those
 *       all-zero services in {@code calendar.txt} exist precisely to be switched on this way.
 * </ul>
 *
 * <p>Exceptions are applied after the weekly pattern, because that is what the spec says and
 * because getting it backwards means running a normal Friday schedule on Independence Day.
 */
public final class ServiceCalendar implements Serializable {

    private static final long serialVersionUID = 1L;

    /** A regular weekly pattern with a validity range. */
    record WeeklyService(
            String serviceId,
            Set<DayOfWeek> days,
            LocalDate startDate,
            LocalDate endDate) implements Serializable {

        boolean runsOn(LocalDate date) {
            return !date.isBefore(startDate)
                    && !date.isAfter(endDate)
                    && days.contains(date.getDayOfWeek());
        }
    }

    private final ImmutableMap<String, WeeklyService> weekly;
    /** date -> service ids explicitly added on that date. */
    private final ImmutableMap<LocalDate, ImmutableSet<String>> added;
    /** date -> service ids explicitly removed on that date. */
    private final ImmutableMap<LocalDate, ImmutableSet<String>> removed;

    ServiceCalendar(Map<String, WeeklyService> weekly,
                    Map<LocalDate, ImmutableSet<String>> added,
                    Map<LocalDate, ImmutableSet<String>> removed) {
        this.weekly = ImmutableMap.copyOf(weekly);
        this.added = ImmutableMap.copyOf(added);
        this.removed = ImmutableMap.copyOf(removed);
    }

    /** An empty calendar treats every service as running, so a feed without these files still works. */
    static ServiceCalendar permissive() {
        return new ServiceCalendar(Map.of(), Map.of(), Map.of());
    }

    /** The services operating on {@code date}, weekly pattern first and then exceptions. */
    public Set<String> activeOn(LocalDate date) {
        if (weekly.isEmpty() && added.isEmpty()) {
            return Set.of(); // permissive calendar: callers should treat empty as "no filter"
        }
        Set<String> active = new HashSet<>();
        for (WeeklyService service : weekly.values()) {
            if (service.runsOn(date)) {
                active.add(service.serviceId());
            }
        }
        active.addAll(added.getOrDefault(date, ImmutableSet.of()));
        active.removeAll(removed.getOrDefault(date, ImmutableSet.of()));
        return active;
    }

    /** True when this calendar carries no information and every service should be allowed. */
    public boolean isPermissive() {
        return weekly.isEmpty() && added.isEmpty() && removed.isEmpty();
    }

    /**
     * Whether {@code date} falls inside the feed's own validity range.
     *
     * <p>Static feeds expire. MARTA's is published for 2026-06-27 to 2026-08-21, and running
     * against a lapsed one means comparing live buses to a schedule the agency has replaced —
     * wrong answers with no error anywhere, which is the worst kind.
     */
    public boolean covers(LocalDate date) {
        if (weekly.isEmpty()) {
            return true;
        }
        return weekly.values().stream()
                .anyMatch(s -> !date.isBefore(s.startDate()) && !date.isAfter(s.endDate()));
    }

    public int serviceCount() {
        return weekly.size();
    }
}
