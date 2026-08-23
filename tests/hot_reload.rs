use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use ct_firehose_filter::{DomainWatchlist, HotWatchlist};

#[test]
fn swap_installs_new_names_and_drops_removed_ones() {
    let hot = HotWatchlist::new(DomainWatchlist::new(["old.com"]));
    assert!(hot.inspect(&["www.old.com"]).is_some());
    assert!(hot.inspect(&["www.new.com"]).is_none());

    hot.swap(DomainWatchlist::new(["new.com"]));

    assert!(
        hot.inspect(&["www.old.com"]).is_none(),
        "removed name must stop matching after swap"
    );
    assert!(hot.inspect(&["www.new.com"]).is_some());
}

#[test]
fn swap_to_empty_watchlist_drops_everything() {
    let hot = HotWatchlist::new(DomainWatchlist::new(["old.com", "new.com"]));
    hot.swap(DomainWatchlist::new(Vec::<String>::new()));
    assert!(hot.inspect(&["www.old.com"]).is_none());
    assert!(hot.inspect(&["www.new.com"]).is_none());
}

#[test]
fn concurrent_inspect_during_swap_does_not_panic_or_tear() {
    let hot = Arc::new(HotWatchlist::new(DomainWatchlist::new(["old.com"])));
    let panics = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for _ in 0..8 {
        let hot = Arc::clone(&hot);
        let panics = Arc::clone(&panics);
        handles.push(thread::spawn(move || {
            for i in 0..5_000 {
                let domain = if i % 2 == 0 {
                    "www.old.com"
                } else {
                    "www.new.com"
                };
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    hot.inspect(&[domain])
                }));
                match result {
                    Ok(ev) => {
                        if let Some(ev) = ev {
                            let ks = &ev.matched_keywords;
                            let only_old = ks == &["old.com".to_string()];
                            let only_new = ks == &["new.com".to_string()];
                            assert!(only_old || only_new, "torn watchlist mix: {ks:?}");
                        }
                    }
                    Err(_) => {
                        panics.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        }));
    }

    for i in 0..200 {
        if i % 2 == 0 {
            hot.swap(DomainWatchlist::new(["new.com"]));
        } else {
            hot.swap(DomainWatchlist::new(["old.com"]));
        }
    }

    for h in handles {
        h.join().expect("worker thread panicked");
    }
    assert_eq!(
        panics.load(Ordering::SeqCst),
        0,
        "inspect panicked during swap"
    );

    hot.swap(DomainWatchlist::new(["new.com"]));
    assert!(hot.inspect(&["www.new.com"]).is_some());
    assert!(hot.inspect(&["www.old.com"]).is_none());
}
