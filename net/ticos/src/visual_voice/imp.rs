// Base on: https://gstreamer-devel.narkive.com/EQbTXvrp/gst-devel-sink-element-with-multiple-sink-pads
// Using request pads (and only creating/adding the elements at that
// time) is the correct way to do it.
// Since you're adding the pads in PAUSED/PLAYING, you will have to
// activate them before adding them to yourself.
// gst_pad_set_active(newpad, TRUE);

// Finally, to make sure your elements are in the right state, once
// you've added them and just before returning the requested pad, you need
// to call 'gst_element_sync_state_with_parent(newelement)' on each of the
// newly added elements. This will ensure they're in the correct state.

use gst::subclass::prelude::*;
use gst::{glib, prelude::*};
use std::sync::{LazyLock, Mutex};

const DEFAULT_THRESHOLD_MS: u32 = 100;

static CAT: LazyLock<gst::DebugCategory> = LazyLock::new(|| {
    gst::DebugCategory::new(
        "visualvoice",
        gst::DebugColorFlags::empty(),
        Some("Filter audio by visual"),
    )
});

#[derive(Debug, Clone)]
pub struct Settings {
    threshold: gst::ClockTime,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            threshold: gst::ClockTime::from_mseconds(DEFAULT_THRESHOLD_MS as u64),
        }
    }
}

pub struct State {
    last_seen: gst::ClockTime,
}

impl Default for State {
    fn default() -> Self {
        Self {
            last_seen: gst::ClockTime::from_seconds(0),
        }
    }
}

pub struct VisualVoice {
    audio_sink_pad: gst::Pad,
    src_pad: gst::Pad,
    settings: Mutex<Settings>,
    state: Mutex<State>,
}

impl VisualVoice {
    fn audio_sink_chain(
        &self,
        _pad: &gst::Pad,
        buffer: gst::Buffer,
    ) -> Result<gst::FlowSuccess, gst::FlowError> {
        if buffer.pts().is_none() {
            gst::error!(CAT, imp = self, "Only buffers with PTS supported");
            return Err(gst::FlowError::Error);
        }
        gst::log!(CAT, imp = self, "Got audio: {:?}", buffer.pts());
        let state = self.state.lock().unwrap();

        // We simply mute the audio if there is no face
        if buffer.pts().unwrap().saturating_sub(state.last_seen)
            > self.settings.lock().unwrap().threshold
        {
            // mute the audio
            let mut buffer = gst::Buffer::with_size(buffer.size()).unwrap();
            {
                let buf = buffer.get_mut().unwrap();
                let mut mem = buf.map_writable().unwrap();
                mem.fill(0);
            }
            self.src_pad.push(buffer)?;
        } else {
            self.src_pad.push(buffer)?;
        }

        Ok(gst::FlowSuccess::Ok)
    }

    fn face_sink_chain(
        &self,
        _pad: &gst::Pad,
        buffer: gst::Buffer,
    ) -> Result<gst::FlowSuccess, gst::FlowError> {
        let map = buffer.map_readable().expect("Failed to read gst::Buffer");
        let tracks: Vec<u64> = bincode::deserialize(&map.as_slice()).unwrap();
        gst::log!(CAT, imp = self, "Got face tracks: {:?}", &tracks);
        if !tracks.is_empty() {
            let mut state = self.state.lock().unwrap();
            state.last_seen = self.obj().current_running_time().unwrap();
        }

        Ok(gst::FlowSuccess::Ok)
    }

    fn sink_event(&self, pad: &gst::Pad, event: gst::Event) -> bool {
        gst::log!(CAT, obj = pad, "Handling event {:?}", event);

        // Lock order: First stream lock then state lock!
        let stream_lock_for_serialized = event.is_serialized();

        let fwd_sticky = stream_lock_for_serialized;

        if fwd_sticky {
            let _ = pad.push_event(gst::event::Reconfigure::new());
            pad.sticky_events_foreach(|event| {
                self.src_pad.push_event(event.clone());
                std::ops::ControlFlow::Continue(gst::EventForeachAction::Keep)
            });
        }

        self.src_pad.push_event(event)
    }
}

#[glib::object_subclass]
impl ObjectSubclass for VisualVoice {
    const NAME: &'static str = "GstVisualVoice";
    type Type = super::VisualVoice;
    type ParentType = gst::Element;
    type Interfaces = (gst::ChildProxy,);

    fn with_class(klass: &Self::Class) -> Self {
        let templ = klass.pad_template("audio").unwrap();
        let audio_sink_pad = gst::Pad::builder_from_template(&templ)
            .chain_function(|pad, parent, buffer| {
                VisualVoice::catch_panic_pad_function(
                    parent,
                    || Err(gst::FlowError::Error),
                    |this| this.audio_sink_chain(pad, buffer),
                )
            })
            .event_function(|pad, parent, event| {
                VisualVoice::catch_panic_pad_function(
                    parent,
                    || false,
                    |this| this.sink_event(pad, event),
                )
            })
            .build();

        let templ = klass.pad_template("src").unwrap();
        let src_pad = gst::Pad::builder_from_template(&templ).build();

        let settings = Mutex::new(Settings::default());

        Self {
            audio_sink_pad,
            src_pad,
            settings,
            state: Default::default(),
        }
    }
}

impl GstObjectImpl for VisualVoice {}

impl ObjectImpl for VisualVoice {
    fn properties() -> &'static [glib::ParamSpec] {
        static PROPERTIES: LazyLock<Vec<glib::ParamSpec>> = LazyLock::new(|| vec![]);

        PROPERTIES.as_ref()
    }

    fn constructed(&self) {
        self.parent_constructed();

        let obj = self.obj();
        obj.add_pad(&self.audio_sink_pad).unwrap();
        obj.add_pad(&self.src_pad).unwrap();
    }

    fn set_property(&self, _id: usize, _value: &glib::Value, pspec: &glib::ParamSpec) {
        match pspec.name() {
            _ => unimplemented!(),
        }
    }

    fn property(&self, _id: usize, pspec: &glib::ParamSpec) -> glib::Value {
        match pspec.name() {
            _ => unimplemented!(),
        }
    }
}

impl ElementImpl for VisualVoice {
    fn metadata() -> Option<&'static gst::subclass::ElementMetadata> {
        static ELEMENT_METADATA: LazyLock<gst::subclass::ElementMetadata> = LazyLock::new(|| {
            gst::subclass::ElementMetadata::new(
                "TicosVisualVoice",
                "Audio/Filter",
                "Filter audio by visual",
                "Alexander Wang<alexander at tiwater dot com>",
            )
        });

        Some(&*ELEMENT_METADATA)
    }

    fn pad_templates() -> &'static [gst::PadTemplate] {
        static PAD_TEMPLATES: LazyLock<Vec<gst::PadTemplate>> = LazyLock::new(|| {
            let audio_caps = gst_audio::AudioCapsBuilder::new_interleaved().build();
            let src_pad_template = gst::PadTemplate::new(
                "src",
                gst::PadDirection::Src,
                gst::PadPresence::Always,
                &audio_caps,
            )
            .unwrap();

            let track_caps = gst::Caps::builder("other/bincode")
                .field("format", "[u64]")
                .build();
            let face_sink_pad_template = gst::PadTemplate::new(
                "face",
                gst::PadDirection::Sink,
                gst::PadPresence::Request,
                &track_caps,
            )
            .unwrap();

            let audio_sink_pad_template = gst::PadTemplate::new(
                "audio",
                gst::PadDirection::Sink,
                gst::PadPresence::Always,
                &audio_caps,
            )
            .unwrap();

            vec![
                src_pad_template,
                face_sink_pad_template,
                audio_sink_pad_template,
            ]
        });

        PAD_TEMPLATES.as_ref()
    }

    fn change_state(
        &self,
        transition: gst::StateChange,
    ) -> Result<gst::StateChangeSuccess, gst::StateChangeError> {
        let mut success = self.parent_change_state(transition)?;

        match transition {
            gst::StateChange::ReadyToPaused => {
                success = gst::StateChangeSuccess::NoPreroll;
            },
            gst::StateChange::PlayingToPaused => {
                success = gst::StateChangeSuccess::NoPreroll;
            },
            _ => (),
        }

        Ok(success)
    }

    fn request_new_pad(
        &self,
        templ: &gst::PadTemplate,
        name: Option<&str>,
        _caps: Option<&gst::Caps>,
    ) -> Option<gst::Pad> {
        gst::debug!(CAT, imp = self, "Request new sink: {:?}", name);

        let pad = gst::Pad::builder_from_template(&templ)
            .chain_function(|pad, parent, buffer| {
                VisualVoice::catch_panic_pad_function(
                    parent,
                    || Err(gst::FlowError::Error),
                    |this| this.face_sink_chain(pad, buffer),
                )
            })
            .build();

        pad.set_active(true).unwrap();
        self.obj().add_pad(&pad).unwrap();

        let _ = self
            .obj()
            .post_message(gst::message::Latency::builder().src(&*self.obj()).build());

        self.obj().child_added(&pad, &pad.name());
        Some(pad)
    }
}

// Implementation of gst::ChildProxy virtual methods.
//
// This allows accessing the pads and their properties from e.g. gst-launch.
impl ChildProxyImpl for VisualVoice {
    fn children_count(&self) -> u32 {
        let object = self.obj();
        object.num_pads() as u32
    }

    fn child_by_name(&self, name: &str) -> Option<glib::Object> {
        let object = self.obj();
        object
            .pads()
            .into_iter()
            .find(|p| p.name() == name)
            .map(|p| p.upcast())
    }

    fn child_by_index(&self, index: u32) -> Option<glib::Object> {
        let object = self.obj();
        object
            .pads()
            .into_iter()
            .nth(index as usize)
            .map(|p| p.upcast())
    }
}
