use gst::glib;

mod object_tracker;
mod visual_voice;
mod websocket_video;

fn plugin_init(plugin: &gst::Plugin) -> Result<(), glib::BoolError> {
    object_tracker::register(plugin)?;
    visual_voice::register(plugin)?;
    websocket_video::register(plugin)?;
    Ok(())
}

gst::plugin_define!(
    ticos,
    env!("CARGO_PKG_DESCRIPTION"),
    plugin_init,
    concat!(env!("CARGO_PKG_VERSION"), "-", env!("COMMIT_ID")),
    "GPL",
    env!("CARGO_PKG_NAME"),
    env!("CARGO_PKG_NAME"),
    env!("CARGO_PKG_REPOSITORY"),
    env!("BUILD_REL_DATE")
);
