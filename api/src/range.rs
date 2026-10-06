use chrono::{DateTime, Utc};
use dlmm_core::domain::ids::UnixSeconds;
use dlmm_core::domain::query::{AlignedRange, Bucket};

const HOUR_SECONDS: i64 = 3_600;
const DAY_SECONDS: i64 = 86_400;
// 31 days of hours and one leap year of days keep a response under a few hundred KB.
const HOUR_BUCKET_COUNT_MAX: i64 = 744;
const DAY_BUCKET_COUNT_MAX: i64 = 366;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeError {
    Empty,
    TooLarge {
        bucket_count: i64,
        bucket_count_max: i64,
    },
}

const fn bucket_seconds(bucket: Bucket) -> i64 {
    match bucket {
        Bucket::Hour => HOUR_SECONDS,
        Bucket::Day => DAY_SECONDS,
    }
}

const fn bucket_count_max(bucket: Bucket) -> i64 {
    match bucket {
        Bucket::Hour => HOUR_BUCKET_COUNT_MAX,
        Bucket::Day => DAY_BUCKET_COUNT_MAX,
    }
}

// Floors `from` and ceils `to` so every requested instant falls inside a whole bucket.
pub fn align_range(
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    bucket: Bucket,
) -> Result<AlignedRange, RangeError> {
    let seconds = bucket_seconds(bucket);
    let from_aligned = from.timestamp().div_euclid(seconds) * seconds;
    // A fractional second past a boundary still reaches into the next bucket.
    let to_seconds = to.timestamp() + i64::from(to.timestamp_subsec_nanos() > 0);
    let to_remainder = to_seconds.rem_euclid(seconds);
    let to_aligned = if to_remainder == 0 {
        to_seconds
    } else {
        to_seconds - to_remainder + seconds
    };
    debug_assert_eq!(from_aligned.rem_euclid(seconds), 0);
    debug_assert_eq!(to_aligned.rem_euclid(seconds), 0);
    if from_aligned >= to_aligned {
        return Err(RangeError::Empty);
    }
    let bucket_count = (to_aligned - from_aligned) / seconds;
    let bucket_count_max = bucket_count_max(bucket);
    if bucket_count > bucket_count_max {
        return Err(RangeError::TooLarge {
            bucket_count,
            bucket_count_max,
        });
    }
    Ok(AlignedRange {
        from: UnixSeconds::new(from_aligned),
        to_exclusive: UnixSeconds::new(to_aligned),
        bucket,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn time(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .expect("test timestamp is valid RFC 3339")
            .with_timezone(&Utc)
    }

    fn aligned(from: &str, to: &str, bucket: Bucket) -> AlignedRange {
        AlignedRange {
            from: UnixSeconds::new(time(from).timestamp()),
            to_exclusive: UnixSeconds::new(time(to).timestamp()),
            bucket,
        }
    }

    #[test]
    fn align_range_floors_from_and_ceils_to_on_utc_boundaries() {
        let (hour, day) = (Bucket::Hour, Bucket::Day);
        let cases = [
            ("2026-10-01T02:30:00+02:00", "2026-10-01T02:59:59Z", hour),
            ("2026-10-01T23:30:00-05:00", "2026-10-03T00:00:00.5Z", day),
            ("2026-09-30T23:10:00Z", "2026-10-01T00:30:00Z", hour),
            ("2026-09-30T12:00:00Z", "2026-10-01T00:00:01Z", day),
            ("2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z", hour),
        ];
        let expected = [
            ("2026-10-01T00:00:00Z", "2026-10-01T03:00:00Z"),
            ("2026-10-02T00:00:00Z", "2026-10-04T00:00:00Z"),
            ("2026-09-30T23:00:00Z", "2026-10-01T01:00:00Z"),
            ("2026-09-30T00:00:00Z", "2026-10-02T00:00:00Z"),
            ("2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z"),
        ];
        for ((from, to, bucket), (expected_from, expected_to)) in cases.into_iter().zip(expected) {
            assert_eq!(
                align_range(time(from), time(to), bucket),
                Ok(aligned(expected_from, expected_to, bucket)),
                "{from} to {to}"
            );
        }
    }

    // from at or after to, once aligned, is rejected rather than answered with no buckets.
    #[test]
    fn reversed_range_is_empty() {
        let range = align_range(
            time("2026-10-01T05:00:00Z"),
            time("2026-10-01T03:00:00Z"),
            Bucket::Hour,
        );
        assert_eq!(range, Err(RangeError::Empty));
    }

    #[test]
    fn bucket_count_caps_are_inclusive() {
        let start = time("2025-01-01T00:00:00Z");
        let hours = |count: i64| start + chrono::Duration::hours(count);
        let days = |count: i64| start + chrono::Duration::days(count);
        assert!(align_range(start, hours(744), Bucket::Hour).is_ok());
        assert_eq!(
            align_range(start, hours(745), Bucket::Hour),
            Err(RangeError::TooLarge {
                bucket_count: 745,
                bucket_count_max: 744
            })
        );
        assert!(align_range(start, days(366), Bucket::Day).is_ok());
        assert_eq!(
            align_range(start, days(367), Bucket::Day),
            Err(RangeError::TooLarge {
                bucket_count: 367,
                bucket_count_max: 366
            })
        );
    }
}
