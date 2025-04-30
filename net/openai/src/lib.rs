use gst::glib;

mod realtime;

fn plugin_init(plugin: &gst::Plugin) -> Result<(), glib::BoolError> {
    realtime::register(plugin)?;
    Ok(())
}

gst::plugin_define!(
    openai,
    env!("CARGO_PKG_DESCRIPTION"),
    plugin_init,
    concat!(env!("CARGO_PKG_VERSION"), "-", env!("COMMIT_ID")),
    "GPL",
    env!("CARGO_PKG_NAME"),
    env!("CARGO_PKG_NAME"),
    env!("CARGO_PKG_REPOSITORY"),
    env!("BUILD_REL_DATE")
);
