use gst::{glib, prelude::StaticType, Rank};

mod imp;

glib::wrapper! {
  pub struct VisualVoice(ObjectSubclass<imp::VisualVoice>) @extends gst::Element, gst::Object, @implements gst::ChildProxy;
}

pub fn register(plugin: &gst::Plugin) -> Result<(), glib::BoolError> {
    gst::Element::register(
        Some(plugin),
        "visualvoice",
        Rank::NONE,
        VisualVoice::static_type(),
    )
}
