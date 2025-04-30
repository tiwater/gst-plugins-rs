mod client_event;
mod executor;
mod imp;
mod server_event;
mod types;

use gst::{glib, prelude::StaticType, Rank};

glib::wrapper! {
  pub struct RealtimeTransformer(ObjectSubclass<imp::RealtimeTransformer>) @extends gst::Element, gst::Object, @implements gst::ChildProxy;
}

glib::wrapper! {
    pub struct RealtimeSrcPad(ObjectSubclass<imp::RealtimeSrcPad>) @extends gst::Pad, gst::Object;
}

pub fn register(plugin: &gst::Plugin) -> Result<(), glib::BoolError> {
    gst::Element::register(
        Some(plugin),
        "openairealtime",
        Rank::NONE,
        RealtimeTransformer::static_type(),
    )
}
