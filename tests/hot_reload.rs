use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use ct_firehose_filter::{HotAutomaton, KeywordAutomaton};

#[test]
fn swap_installs_new_keywords_and_drops_removed_ones() {
    let hot = HotAutomaton::new(KeywordAutomaton::new(["alpha"]));
    assert!(hot.inspect(&["alpha.com"]).is_some());
    assert!(hot.inspect(&["beta.com"]).is_none());

    hot.swap(KeywordAutomaton::new(["beta"]));

    assert!(
        hot.inspect(&["alpha.com"]).is_none(),
        "removed keyword must stop matching after swap"
    );
    assert!(hot.inspect(&["beta.com"]).is_some());
}

#[test]
fn swap_to_empty_automaton_drops_everything() {
    let hot = HotAutomaton::new(KeywordAutomaton::new(["alpha", "beta"]));
    hot.swap(KeywordAutomaton::new(Vec::<String>::new()));
    assert!(hot.inspect(&["alpha.com"]).is_none());
    assert!(hot.inspect(&["beta.com"]).is_none());
}

#[test]
fn concurrent_inspect_during_swap_does_not_panic_or_tear() {
    let hot = Arc::new(HotAutomaton::new(KeywordAutomaton::new(["oldkw"])));
    let panics = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for _ in 0..8 {
        let hot = Arc::clone(&hot);
        let panics = Arc::clone(&panics);
        handles.push(thread::spawn(move || {
            for i in 0..5_000 {
                let domain = if i % 2 == 0 {
                    "oldkw.example.com"
                } else {
                    "newkw.example.com"
                };
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    hot.inspect(&[domain])
                }));
                match result {
                    Ok(ev) => {
                        if let Some(ev) = ev {
                            // Must be a coherent automaton: never mix old+new keywords.
                            let ks = &ev.matched_keywords;
                            let only_old = ks == &["oldkw".to_string()];
                            let only_new = ks == &["newkw".to_string()];
                            assert!(only_old || only_new, "torn keyword mix: {ks:?}");
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
            hot.swap(KeywordAutomaton::new(["newkw"]));
        } else {
            hot.swap(KeywordAutomaton::new(["oldkw"]));
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

    hot.swap(KeywordAutomaton::new(["newkw"]));
    assert!(hot.inspect(&["newkw.example.com"]).is_some());
    assert!(hot.inspect(&["oldkw.example.com"]).is_none());
}
