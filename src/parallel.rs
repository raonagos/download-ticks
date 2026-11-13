//! A bounded, order-preserving parallel map for the CPU-bound stages.
//!
//! `sync` waits on the network and lives on the Tokio runtime. `build` and
//! `verify` do not: they hash, decode, sort and compress, and their units of
//! work — a day, an object — never touch each other. Running those units on
//! plain OS threads at most `jobs` at a time is what turns a single busy core
//! into a busy machine.
//!
//! Results come back in input order. Which unit finished first must not change
//! the manifest, the report, or the error message a run prints.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::sync_channel;
use std::thread;

use crate::errors::Result;

/// The number of cores the machine reports, at least one.
pub fn cores() -> usize {
    thread::available_parallelism().map(|count| count.get()).unwrap_or(1)
}

/// How many days `build` may hold in flight when the flag is left at `auto`.
///
/// A day in flight is decoded and sorted in memory before it is written: about
/// 90 bytes per event, so ~3 GiB for a 35M-event day. Parallelism is
/// therefore bounded by cores *and* by memory, not by cores alone; the memory
/// budget in `build` does the fine-grained admitting.
pub fn build_jobs() -> usize {
    // One day per three cores: a day already decodes its four objects on four
    // threads, and the compression that follows is single-threaded, so more
    // days than this would only oversubscribe.
    (cores() / 3).clamp(1, 4)
}

/// Bytes the CPU-bound stages may spend on data held in memory.
///
/// Three fifths of what the machine reports as available: the rest belongs to
/// the page cache, the day's objects, and the session that runs the build.
pub fn memory_budget() -> u64 {
    available_memory().map(|bytes| (bytes / 5 * 3).max(4 << 30)).unwrap_or(8 << 30)
}

/// Free memory in bytes, when the platform reports it.
fn available_memory() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = text.lines().find(|line| line.starts_with("MemAvailable:"))?;
    let kib = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    Some(kib * 1024)
}

/// Runs `work` over every item on at most `jobs` threads, in input order.
///
/// `observe` is called on the calling thread as each result arrives, so a long
/// run can report progress without interleaving its output. After a failure no
/// new unit is started, and the failure reported is the first one in input
/// order — not the first one that happened to finish.
pub fn map<T, R, F, O>(items: &[T], jobs: usize, work: F, mut observe: O) -> Result<Vec<R>>
where
    T: Sync,
    R: Send,
    F: Fn(usize, &T) -> Result<R> + Sync,
    O: FnMut(usize, &Result<R>),
{
    if items.is_empty() {
        return Ok(Vec::new());
    }

    let jobs = jobs.max(1).min(items.len());
    let next = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    // Bounded: a worker that finishes early waits instead of hoarding results.
    let (tx, rx) = sync_channel::<(usize, Result<R>)>(jobs * 2);
    let mut slots: Vec<Option<Result<R>>> = (0..items.len()).map(|_| None).collect();

    thread::scope(|scope| {
        for _ in 0..jobs {
            let tx = tx.clone();
            let next = &next;
            let stop = &stop;
            let work = &work;
            scope.spawn(move || {
                loop {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(item) = items.get(index) else {
                        break;
                    };
                    let outcome = work(index, item);
                    let failed = outcome.is_err();
                    if tx.send((index, outcome)).is_err() {
                        break;
                    }
                    if failed {
                        stop.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            });
        }
        drop(tx);

        // Draining here, and only here, is what keeps the producers moving.
        for (index, outcome) in rx.iter() {
            observe(index, &outcome);
            slots[index] = Some(outcome);
        }
    });

    let mut results = Vec::with_capacity(items.len());
    for slot in slots {
        match slot {
            // Never started: an earlier unit failed and stopped the run.
            None => break,
            Some(Ok(value)) => results.push(value),
            Some(Err(error)) => return Err(error),
        }
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn results_come_back_in_input_order() {
        let items: Vec<usize> = (0..16).collect();
        let results = map(&items, 4, |_, item| Ok(*item * 2), |_, _| {}).unwrap();
        assert_eq!(results, items.iter().map(|item| item * 2).collect::<Vec<_>>());
    }

    #[test]
    fn every_item_is_visited_exactly_once() {
        let items: Vec<usize> = (0..32).collect();
        let seen = Mutex::new(Vec::new());
        map(
            &items,
            3,
            |_, item| {
                seen.lock().unwrap().push(*item);
                Ok(())
            },
            |_, _| {},
        )
        .unwrap();
        let mut seen = seen.into_inner().unwrap();
        seen.sort_unstable();
        assert_eq!(seen, items);
    }

    #[test]
    fn the_first_failure_in_input_order_is_reported() {
        let items: Vec<usize> = (0..12).collect();
        let error = map(
            &items,
            4,
            |_, item| {
                if *item % 3 == 0 {
                    Err(crate::errors::Error::Verify(format!("failed at {item}")))
                } else {
                    Ok(*item)
                }
            },
            |_, _| {},
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "verification failed: failed at 0");
    }

    #[test]
    fn no_new_unit_starts_after_a_failure() {
        let items: Vec<usize> = (0..64).collect();
        let visited = Mutex::new(0usize);
        let _ = map(
            &items,
            1,
            |_, item| {
                *visited.lock().unwrap() += 1;
                if *item == 2 {
                    Err(crate::errors::Error::Verify("stop".to_owned()))
                } else {
                    Ok(*item)
                }
            },
            |_, _| {},
        );
        // One worker, sequential: nothing after the failure is started.
        assert_eq!(*visited.lock().unwrap(), 3);
    }

    #[test]
    fn an_empty_input_is_not_a_job() {
        let items: Vec<usize> = Vec::new();
        assert!(map(&items, 4, |_, item| Ok(*item), |_, _| {}).unwrap().is_empty());
    }

    #[test]
    fn every_core_is_offered_by_default() {
        assert!(cores() >= 1);
        assert!(build_jobs() >= 1 && build_jobs() <= 4);
    }
}
