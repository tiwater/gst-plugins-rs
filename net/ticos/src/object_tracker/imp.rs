use gst::subclass::prelude::*;
use gst::{glib, prelude::*};
use nnstreamer_rs::TensorMetaInfo;
use similari::prelude::PositionalMetricType::IoU;
use similari::prelude::{Sort, Universal2DBox};
use similari::trackers::sort::metric::DEFAULT_MINIMAL_SORT_CONFIDENCE;
use similari::trackers::sort::DEFAULT_SORT_IOU_THRESHOLD;
use similari::utils::bbox::BoundingBox;
use std::sync::{LazyLock, Mutex};

static CAT: LazyLock<gst::DebugCategory> = LazyLock::new(|| {
    gst::DebugCategory::new(
        "objecttracker",
        gst::DebugColorFlags::empty(),
        Some("High-performance real-time multiple object trackers"),
    )
});

pub struct Settings {}

impl Default for Settings {
    fn default() -> Self {
        Self {}
    }
}

pub struct State {
    tracker: Sort,
}

impl Default for State {
    fn default() -> Self {
        Self {
            tracker: Sort::new(
                1,
                10,
                1,
                IoU(DEFAULT_SORT_IOU_THRESHOLD),
                DEFAULT_MINIMAL_SORT_CONFIDENCE,
                None,
                1.0 / 20.0,
                1.0 / 160.0,
            ),
        }
    }
}

pub struct ObjectTracker {
    sinkpad: gst::Pad,
    srcpad: gst::Pad,
    // settings: Mutex<Settings>,
    state: Mutex<State>,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct TensorBBox {
    x: u32,
    y: u32,
    w: u32,
    h: u32,
}

// Implementation of gst::ChildProxy virtual methods.
//
// This allows accessing the pads and their properties from e.g. gst-launch.
impl ChildProxyImpl for ObjectTracker {
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

impl ObjectTracker {
    fn sink_chain(
        &self,
        _pad: &gst::Pad,
        buffer: gst::Buffer,
    ) -> Result<gst::FlowSuccess, gst::FlowError> {
        if buffer.pts().is_none() {
            gst::error!(CAT, imp = self, "Only buffers with PTS supported");
            return Err(gst::FlowError::Error);
        }

        let map = buffer.map_readable().expect("Failed to read gst::Buffer");
        if map.size() >= std::mem::size_of::<TensorMetaInfo>() {
            let mut ptr = map.as_ptr();
            let tensor_meta = unsafe { *(ptr as *const TensorMetaInfo) };
            gst::debug!(CAT, imp = self, "Meta: {:?}", tensor_meta);

            ptr = unsafe { ptr.add(tensor_meta.get_header_size()) };

            let mut bboxes: Vec<(Universal2DBox, Option<i64>)> = Vec::new();
            for _ in 0..tensor_meta.dimension[1] {
                let bbox = unsafe { *(ptr as *const TensorBBox) };
                let bbox =
                    BoundingBox::new(bbox.x as f32, bbox.y as f32, bbox.w as f32, bbox.h as f32);
                gst::trace!(CAT, imp = self, "bbox: {:?}", bbox);
                bboxes.push((bbox.into(), None));

                ptr = unsafe {
                    ptr.add(tensor_meta.dimension[0] as usize * std::mem::size_of::<u32>())
                };
            }

            let mut state = self.state.lock().unwrap();
            let tracks = state.tracker.predict(&bboxes);
            // Make it simple, use a vector to represent the track id
            let track_ids: Vec<u64> = tracks.iter().map(|x| x.id).collect();
            gst::debug!(CAT, imp = self, "Track ids: {:?}", track_ids);

            let byte_data: Vec<u8> = bincode::serialize(&track_ids).unwrap();
            let buffer = gst::Buffer::from_slice(byte_data);
            self.srcpad.push(buffer)?;
        }

        Ok(gst::FlowSuccess::Ok)
    }
}

#[glib::object_subclass]
impl ObjectSubclass for ObjectTracker {
    const NAME: &'static str = "GstObjectTracker";
    type Type = super::ObjectTracker;
    type ParentType = gst::Element;
    type Interfaces = (gst::ChildProxy,);

    fn with_class(klass: &Self::Class) -> Self {
        let templ = klass.pad_template("sink").unwrap();
        let sinkpad = gst::Pad::builder_from_template(&templ)
            .chain_function(|pad, parent, buffer| {
                ObjectTracker::catch_panic_pad_function(
                    parent,
                    || Err(gst::FlowError::Error),
                    |tracker| tracker.sink_chain(pad, buffer),
                )
            })
            .build();

        let templ = klass.pad_template("src").unwrap();
        let srcpad = gst::Pad::builder_from_template(&templ).build();

        // let settings = Mutex::new(Settings::default());

        Self {
            sinkpad,
            srcpad,
            // settings,
            state: Default::default(),
        }
    }
}

impl GstObjectImpl for ObjectTracker {}

impl ObjectImpl for ObjectTracker {
    fn properties() -> &'static [glib::ParamSpec] {
        static PROPERTIES: LazyLock<Vec<glib::ParamSpec>> = LazyLock::new(|| vec![]);

        PROPERTIES.as_ref()
    }

    fn constructed(&self) {
        self.parent_constructed();

        let obj = self.obj();
        obj.add_pad(&self.sinkpad).unwrap();
        obj.add_pad(&self.srcpad).unwrap();
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

impl ElementImpl for ObjectTracker {
    fn metadata() -> Option<&'static gst::subclass::ElementMetadata> {
        static ELEMENT_METADATA: LazyLock<gst::subclass::ElementMetadata> = LazyLock::new(|| {
            gst::subclass::ElementMetadata::new(
                "TicosObjectTracker",
                "Converter",
                "High-performance real-time multiple object trackers",
                "Alexander Wang<alexander at tiwater dot com>",
            )
        });

        Some(&*ELEMENT_METADATA)
    }

    fn pad_templates() -> &'static [gst::PadTemplate] {
        static PAD_TEMPLATES: LazyLock<Vec<gst::PadTemplate>> = LazyLock::new(|| {
            // Today, the Realtime API supports two formats:
            // raw 16 bit PCM audio at 24kHz, 1 channel, little-endian
            // G.711 at 8kHz (both u-law and a-law)
            let bincode_caps = gst::Caps::builder("other/bincode")
                .field("format", "[u64]")
                .build();
            let src_pad_template = gst::PadTemplate::new(
                "src",
                gst::PadDirection::Src,
                gst::PadPresence::Always,
                &bincode_caps,
            )
            .unwrap();

            let tensor_caps = gst::Caps::builder("other/tensors")
                .field("format", "flexible")
                .build();
            let sink_pad_template = gst::PadTemplate::new(
                "sink",
                gst::PadDirection::Sink,
                gst::PadPresence::Always,
                &tensor_caps,
            )
            .unwrap();

            vec![src_pad_template, sink_pad_template]
        });

        PAD_TEMPLATES.as_ref()
    }
}
