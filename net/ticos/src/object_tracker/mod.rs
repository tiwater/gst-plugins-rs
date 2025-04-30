use gst::{glib, prelude::StaticType, Rank};

mod imp;

glib::wrapper! {
  pub struct ObjectTracker(ObjectSubclass<imp::ObjectTracker>) @extends gst::Element, gst::Object, @implements gst::ChildProxy;
}

pub fn register(plugin: &gst::Plugin) -> Result<(), glib::BoolError> {
    gst::Element::register(
        Some(plugin),
        "objecttracker",
        Rank::NONE,
        ObjectTracker::static_type(),
    )
}
