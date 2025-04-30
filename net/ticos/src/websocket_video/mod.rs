mod imp;

use gst::{glib, prelude::StaticType, Rank};

glib::wrapper! {
    pub struct WebsocketVideoSink(ObjectSubclass<imp::WebsocketVideoSink>) @extends gst::Element, gst::Object;
}

pub fn register(plugin: &gst::Plugin) -> Result<(), glib::BoolError> {
    gst::Element::register(
        Some(plugin),
        "websocketvideosink",
        Rank::NONE,
        WebsocketVideoSink::static_type(),
    )
} 