use std::sync::Once;

static PANIC_HOOK: Once = Once::new();

pub fn install_panic_hook() {
    PANIC_HOOK.call_once(|| {
        std::panic::set_hook(Box::new(|info| {
            let current = std::thread::current();
            let thread = current.name().unwrap_or("unnamed");
            let payload = info
                .payload()
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| info.payload().downcast_ref::<String>().map(|s| s.as_str()))
                .unwrap_or("<non-string payload>");
            let backtrace = std::backtrace::Backtrace::force_capture();
            if let Some(loc) = info.location() {
                tracing::error!(
                    "panic on thread '{}' at {}:{}:{}: {}\nbacktrace:\n{}",
                    thread,
                    loc.file(),
                    loc.line(),
                    loc.column(),
                    payload,
                    backtrace
                );
            } else {
                tracing::error!(
                    "panic on thread '{}': {}\nbacktrace:\n{}",
                    thread,
                    payload,
                    backtrace
                );
            }
        }));
    });
}
