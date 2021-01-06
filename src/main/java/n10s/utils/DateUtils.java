package n10s.utils;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Calendar;

import javax.xml.bind.DatatypeConverter;

public class DateUtils {
	/**
	 * Convert a String-formatted date into a LocalDateTime object by using
	 * LocalDateTime.parse first, and DatatypeConverter.parseDateTime if the
	 * first one fails.
	 * @param stringDateTime
	 *            The string-formatted date.
	 * @return LocalDateTime object.
	 * @throws IllegalArgumentException if the string is not parseable to a date.
	 */
	public static LocalDateTime parseDateTime(String stringDateTime) {
		boolean dateParsed = false;
		LocalDateTime localDateTime = null;
		StringBuilder parserErrors = new StringBuilder("Error parsing ").append(stringDateTime).append(":\n");

		/* Try date parsing with LocalDateTime.parse */
		try {
			localDateTime = LocalDateTime.parse(stringDateTime);
			dateParsed = true;
		} catch (DateTimeParseException e) {
			dateParsed = false;
			parserErrors.append(e.getMessage()).append("\n");
		}

		/* If date is not parsed */
		if (!dateParsed) {
			/* Try with DatatypeConverter.parseDateTime */
			try {
				Calendar calendar = DatatypeConverter.parseDateTime(stringDateTime);
				localDateTime = LocalDateTime.ofInstant(calendar.toInstant(), calendar.getTimeZone().toZoneId());
				dateParsed = true;
			} catch (IllegalArgumentException | DateTimeException e) {
				dateParsed = false;
				parserErrors.append(e.getMessage()).append("\n");
			}
		}

		/* If date is not parsed, throw exception */
		if (!dateParsed) {
			throw new IllegalArgumentException(parserErrors.toString());
		} else {
			return localDateTime;
		}
	}

	public static LocalDate parseDate(String stringDate) {
		boolean dateParsed = false;
		LocalDate localDate = null;
		StringBuilder parserErrors = new StringBuilder("Error parsing ").append(stringDate).append(":\n");

		/* Try date parsing with LocalDateTime.parse */
		try {
			localDate = LocalDate.parse(stringDate);
			dateParsed = true;
		} catch (DateTimeParseException e) {
			dateParsed = false;
			parserErrors.append(e.getMessage()).append("\n");
		}

		/* If date is not parsed */
		if (!dateParsed) {
			/* Try with DatatypeConverter.parseDateTime */
			try {
				Calendar calendar = DatatypeConverter.parseDate(stringDate);
				localDate = LocalDate.ofInstant(calendar.toInstant(), calendar.getTimeZone().toZoneId());
				dateParsed = true;
			} catch (IllegalArgumentException | DateTimeException e) {
				dateParsed = false;
				parserErrors.append(e.getMessage()).append("\n");
			}
		}

		/* If date is not parsed, throw exception */
		if (!dateParsed) {
			throw new IllegalArgumentException(parserErrors.toString());
		} else {
			return localDate;
		}
	}
}
