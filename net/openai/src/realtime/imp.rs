use async_tungstenite::tungstenite::error::Error as WsError;
use async_tungstenite::{tokio::connect_async, tungstenite::Message};
use atomic_refcell::AtomicRefCell;
use base64::prelude::*;
use futures::channel::mpsc;
use futures::future::{abortable, AbortHandle};
use futures::prelude::*;
use gst::subclass::prelude::*;
use gst::{glib, prelude::*};
use http::Request;
use std::collections::{BTreeSet, VecDeque};
use std::default::Default;
use std::pin::Pin;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::Duration;
use tokio::runtime;
use url::Url;
use futures_util::StreamExt;

use super::client_event;
use super::executor::*;
use super::server_event;
use super::types::*;

#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
struct RealtimeEvent {
    #[serde(rename = "type")]
    type_: String,
}

static CAT: LazyLock<gst::DebugCategory> = LazyLock::new(|| {
    gst::DebugCategory::new(
        "openairealtime",
        gst::DebugColorFlags::empty(),
        Some("Audio to audio transformer using the OpenAI Realtime API"),
    )
});

static RUNTIME: LazyLock<runtime::Runtime> = LazyLock::new(|| {
    runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .build()
        .unwrap()
});

const SAMPLE_RATE: u32 = 24000;
const CHANNELS: u32 = 1;
const GRANULARITY_MS: u32 = 100;

const BYTES_PER_SAMPLE: u32 = 2;
const CHUNK_SIZE: u32 = 200 /*ms*/ * (SAMPLE_RATE / 1000) * CHANNELS * BYTES_PER_SAMPLE;

const DEFAULT_BUFFER_TIME_MS: u32 = 500;
const DEFAULT_VAD_THRESHOLD: f32 = 0.09;
const DEFAULT_VAD_PREFIX_PADDING_MS: u32 = 300;
const DEFAULT_VAD_SILENCE_DURATION_MS: u32 = 520;
const DEFAULT_VAD_MIN_AUDIO_DURATION_MS: u32 = 800;
const DEFAULT_RECONNECT_ATTEMPTS: u32 = 3;
const DEFAULT_RECONNECT_DELAY_MS: u64 = 5000;
const DEFAULT_CONNECTION_TIMEOUT_MS: u64 = 5000;

fn calculate_audio_length_ms(data_size: usize) -> u64 {
    // by ms
    (data_size as f64 * 1000 as f64
        / (SAMPLE_RATE as f64 * CHANNELS as f64 * BYTES_PER_SAMPLE as f64)) as u64
}

fn create_sine_wave(duration_ms: u32) -> Vec<u8> {
    let freq = 440.0;
    let step = std::f64::consts::PI * 2.0 * freq / SAMPLE_RATE as f64;
    let amp = 32767.0;
    let mut buffer: Vec<u8> = Vec::new();
    let n_samples = (duration_ms * (SAMPLE_RATE / 1000) * CHANNELS) as usize;

    let mut accumulator = 0.0;
    for _ in 0..n_samples {
        accumulator += step;
        if accumulator >= std::f64::consts::PI * 2.0 {
            accumulator -= std::f64::consts::PI * 2.0;
        }
        // S16LE
        let bytes = ((accumulator.sin() * amp) as i16).to_le_bytes();
        buffer.extend(bytes);
    }

    buffer
}

#[derive(Debug, Clone)]
struct Settings {
    url: Option<String>,
    api_key: Option<String>,
    agent_id: Option<String>,
    buffer_time_ms: u32,
    voice: Option<String>,
    model_name: Option<String>,
    model_provider: Option<String>,
    instructions: Option<String>,
    // VAD related
    vad_threshold: f32,
    vad_silence_duration_ms: u32,
    vad_min_audio_duration: gst::ClockTime,
    server_vad: bool,

    executor_addr: Option<String>,
    knowledge_scripts: Vec<String>,
    tools: Option<Vec<ToolDefinition>>,
    
    // Vision related
    vision_enable_face_detection: bool,
    vision_enable_face_identification: bool,
    vision_enable_object_detection: bool,
    vision_object_detection_classes: Vec<String>,
    
    // WebSocket连接相关设置
    reconnect_attempts: u32,
    reconnect_delay_ms: u64,
    connection_timeout_ms: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            url: Some("wss://openai-realtime-ws.ticos.ai".to_string()),
            api_key: None,
            agent_id: None,
            buffer_time_ms: DEFAULT_BUFFER_TIME_MS,
            voice: None,
            model_name: None,
            model_provider: None,
            instructions: None,
            vad_threshold: DEFAULT_VAD_THRESHOLD,
            vad_silence_duration_ms: DEFAULT_VAD_SILENCE_DURATION_MS,
            vad_min_audio_duration: gst::ClockTime::from_mseconds(
                DEFAULT_VAD_MIN_AUDIO_DURATION_MS as u64,
            ),
            server_vad: false,
            executor_addr: None,
            knowledge_scripts: Vec::new(),
            tools: None,
            // Vision related defaults
            vision_enable_face_detection: false,
            vision_enable_face_identification: false,
            vision_enable_object_detection: false,
            vision_object_detection_classes: Vec::new(),
            reconnect_attempts: DEFAULT_RECONNECT_ATTEMPTS,
            reconnect_delay_ms: DEFAULT_RECONNECT_DELAY_MS,
            connection_timeout_ms: DEFAULT_CONNECTION_TIMEOUT_MS,
        }
    }
}
struct State {
    connected: bool,
    recv_abort_handle: Option<AbortHandle>,
    send_abort_handle: Option<AbortHandle>,
    in_segment: gst::FormattedSegment<gst::ClockTime>,
    acc_audio_duration: gst::ClockTime,
    pad_serial: u32,
    srcpads: BTreeSet<super::RealtimeSrcPad>,
    vad_abort_handle: Option<AbortHandle>,
}
impl State {}

impl Default for State {
    fn default() -> Self {
        Self {
            connected: false,
            recv_abort_handle: None,
            send_abort_handle: None,
            in_segment: gst::FormattedSegment::new(),
            acc_audio_duration: gst::ClockTime::from_seconds(0),
            pad_serial: 0,
            srcpads: BTreeSet::new(),
            vad_abort_handle: None,
        }
    }
}

type WsSink = Pin<Box<dyn Sink<Message, Error = WsError> + Send + Sync>>;

pub struct RealtimeTransformer {
    sinkpad: gst::Pad,
    settings: Mutex<Settings>,
    state: Mutex<State>,
    ws_sink: AtomicRefCell<Option<WsSink>>,
}

impl RealtimeSrcPad {
    fn dequeue(&self) -> bool {
        let Some(parent) = self.obj().parent() else {
            return true;
        };

        let transformer = parent
            .downcast::<super::RealtimeTransformer>()
            .expect("parent is transformer");

        let Some(now) = transformer.current_running_time() else {
            return true;
        };

        // 安全地获取settings锁
        let buffer_time = match transformer.imp().settings.lock() {
            Ok(settings) => gst::ClockTime::from_mseconds(settings.buffer_time_ms as u64),
            Err(err) => {
                gst::error!(CAT, imp = self, "Failed to acquire settings lock: {}", err);
                return false;
            }
        };

        // 首先检查并发送所有必要的事件，确保在推送缓冲区前发送
        let events = {
            // 安全地获取state锁
            let state_guard = match self.state.lock() {
                Ok(guard) => guard,
                Err(err) => {
                    gst::error!(CAT, imp = self, "Failed to acquire state lock: {}", err);
                    return false;
                }
            };
            
            let mut events = vec![];
            
            if self.obj().sticky_event::<gst::event::StreamStart>(0).is_none() {
                let stream_id = format!("audio_{}", self.obj().name());
                events.push(
                    gst::event::StreamStart::builder(&stream_id)
                        .seqnum(state_guard.seqnum)
                        .build(),
                );
            }
            
            if !self.obj().has_current_caps() {
                let caps = gst_audio::AudioCapsBuilder::new()
                    .format(gst_audio::AudioFormat::S16le)
                    .rate(SAMPLE_RATE as i32)
                    .channels(CHANNELS as i32)
                    .layout(gst_audio::AudioLayout::Interleaved)
                    .build();
                events.push(
                    gst::event::Caps::builder(&caps)
                        .seqnum(state_guard.seqnum)
                        .build(),
                );
            }
            
            if self.obj().sticky_event::<gst::event::Segment>(0).is_none() {
                gst::debug!(
                    CAT,
                    imp = self,
                    "Constructing segment event from {:?}",
                    state_guard.out_segment
                );
                events.push(
                    gst::event::Segment::builder(&state_guard.out_segment)
                        .seqnum(state_guard.seqnum)
                        .build(),
                );
            }
            
            events
        };
        
        for event in events {
            gst::debug!(CAT, imp = self, "Pushing event {event:?}");
            self.obj().push_event(event);
        }

        let mut items = vec![];
        let granularity = gst::ClockTime::from_mseconds(GRANULARITY_MS as u64);

        // 安全地获取state锁并处理缓冲区
        let (buffer_time, now, mut last_position, send_eos, seqnum) = {
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(err) => {
                    gst::error!(CAT, imp = self, "Failed to acquire state lock: {}", err);
                    return false;
                }
            };

            let send_eos = state.send_eos && state.buffers.is_empty();

            while let Some(buf) = state.buffers.front() {
                if buf.pts().unwrap().saturating_sub(now) < granularity + buffer_time {
                    let mut buf = state.buffers.pop_front().unwrap();
                    {
                        let buf_mut = buf.make_mut();
                        let mut pts = buf_mut.pts().unwrap();
                        let mut duration = buf_mut.duration().unwrap();
                        if let Some(position) = state.out_segment.position() {
                            if pts < position {
                                gst::debug!(
                                    CAT,
                                    imp = self,
                                    "Adjusting item timing({:?} < {:?})",
                                    pts,
                                    position,
                                );
                                duration = duration.saturating_sub(position - pts);
                                pts = position;
                            }
                        }

                        buf_mut.set_pts(pts);
                        buf_mut.set_duration(duration);
                    }

                    items.push(buf);
                } else {
                    break;
                }
            }

            (
                buffer_time,
                now,
                state.out_segment.position(),
                send_eos,
                state.seqnum,
            )
        };

        if send_eos {
            let _ = self.obj().pause_task();
            return self
                .obj()
                .push_event(gst::event::Eos::builder().seqnum(seqnum).build());
        }

        for buf in items.drain(..) {
            let pts = buf.pts().unwrap();
            let pts_end = if let Some(duration) = buf.duration() {
                pts + duration
            } else {
                pts
            };
            last_position = Some(pts_end);

            gst::debug!(
                CAT,
                imp = self,
                "Pushing audio buffer: {} -> {}",
                pts,
                pts_end,
            );

            if self.obj().push(buf).is_err() {
                return false;
            }
        }

        if let Some(last_position_) = last_position {
            if now >= last_position_ && now - last_position_ + granularity > buffer_time {
                let duration = now - last_position_ + granularity;
                last_position = Some(last_position_ + duration);
            }

            // 安全地获取state锁并更新position
            if let Ok(mut state) = self.state.lock() {
                state.out_segment.set_position(last_position);
            }
        }

        true
    }

    fn enqueue_audio(&self, state: &mut RealtimeSrcPadState, audio_delta: &String) {
        gst::log!(CAT, "Enqueuing {:?}", audio_delta.len());

        let Some(parent) = self.obj().parent() else {
            return;
        };

        let transformer = parent
            .downcast::<super::RealtimeTransformer>()
            .expect("parent is transformer");

        let Some(now) = transformer.current_running_time() else {
            return;
        };

        let last_position = state.last_buffer_rtime.unwrap_or(now);

        match BASE64_STANDARD.decode(audio_delta) {
            Ok(decoded_bytes) => {
                let mut start_time = std::cmp::max(now, last_position);
                for chunk in decoded_bytes.chunks(CHUNK_SIZE as usize) {
                    let duration =
                        gst::ClockTime::from_mseconds(calculate_audio_length_ms(chunk.len()));
                    let mut buf = gst::Buffer::from_mut_slice(chunk.to_vec());
                    {
                        let buf = buf.get_mut().unwrap();
                        buf.set_pts(start_time);
                        buf.set_duration(duration);
                    }
                    state.push_buffer(buf);

                    start_time += duration;
                }
            },
            Err(e) => {
                gst::log!(CAT, "Error decoding Base64: {}", e);
            },
        }
    }

    fn loop_fn(&self, receiver: &mut mpsc::Receiver<Message>) -> Result<(), gst::ErrorMessage> {
        let future = async move {
            let msg = match receiver.next().await {
                Some(msg) => msg,
                /* Sender was closed */
                None => {
                    let _ = self.obj().pause_task();
                    return Ok(());
                },
            };

            match msg {
                Message::Text(_) => {
                    gst::trace!(CAT, "server event: {}", msg);

                    let data = msg.clone().into_data();
                    let event = match serde_json::from_slice::<server_event::ServerEvent>(&data) {
                        Ok(val) => val,
                        Err(e) => {
                            gst::warning!(
                                CAT,
                                "Error when parsing server message, {:?} : {}",
                                e,
                                msg
                            );
                            return Ok(());
                        },
                    };
                    match event {
                        server_event::ServerEvent::ResponseAudioDelta(audio_delta) => {
                            /* This pad outputs audio */
                            gst::debug!(
                                CAT,
                                imp = self,
                                "Response audio delta, event_id = {}, len = {}, item_id = {}",
                                audio_delta.event_id,
                                audio_delta.delta.len(),
                                audio_delta.item_id,
                            );

                            let mut state = self.state.lock().unwrap();
                            if !audio_delta.delta.is_empty() && state.current_item_id.as_ref().is_some_and(|x| *x == audio_delta.item_id) {
                                self.enqueue_audio(&mut state, &audio_delta.delta);
                            } else {
                                gst::warning!(CAT, imp = self, "Ignore AudioDelta as the item_id is invalid: {}", audio_delta.item_id);
                            }
                        },
                        server_event::ServerEvent::Error(e) => {
                            gst::error!(
                                CAT,
                                imp = self,
                                "Got error from server: {} ({})",
                                e.error.r#type,
                                e.error.message
                            );
                        },
                        server_event::ServerEvent::ResponseCreated(resp) => {
                            let mut state = self.state.lock().unwrap();
                            match resp.response.output.first() {
                                Some(ResponseOutputItem::ItemId(item_id)) => {
                                    gst::debug!(CAT, imp = self, "Created response with id: {}", item_id);
                                    state.current_item_id = Some(item_id.clone());
                                },
                                Some(ResponseOutputItem::Item(_)) => {
                                    assert!(false);
                                },
                                None => {
                                    state.current_item_id = None;
                                }
                            }
                        },
                        server_event::ServerEvent::InputAudioBufferSpeechStopped(_) => {
                            let mut state = self.state.lock().unwrap();
                            gst::debug!(CAT, imp = self, "Stop response for id: {:?}, clean buffer of {} items", state.current_item_id, state.buffers.len());
                            state.reset_buffers();
                        },
                        server_event::ServerEvent::ResponseOutputItemAdded(output_item) => {
                            gst::debug!(CAT, imp = self, "response.output_item.added: {:?}", output_item);
                            let mut state = self.state.lock().unwrap();
                            let parent = self
                                .obj()
                                .parent()
                                .and_downcast::<super::RealtimeTransformer>()
                                .expect("has parent");
                            let settings = parent.imp().settings.lock().unwrap();
                            
                            // 检查executor_addr是否已设置
                            if settings.executor_addr.is_none() {
                                gst::debug!(CAT, imp = self, "Executor address not set, skipping connection");
                                return Ok(());
                            }
                            
                            // 如果之前的连接失败了，先重置连接
                            if !state.executor.is_connected() {
                                match state.executor.ensure_connection(&settings.executor_addr) {
                                    Ok(_) => {
                                        gst::debug!(CAT, imp = self, "Successfully connected to executor");
                                    },
                                    Err(err) => {
                                        gst::error!(CAT, imp = self, "Failed to connect executor: {}", err);
                                    },
                                }
                            }
                        },
                        server_event::ServerEvent::ResponseFunctionCallArgumentsDone(fc_arguments_done) => {
                            gst::debug!(CAT, imp = self, "response.function_call_arguments.done: {:?}", fc_arguments_done);
                            let mut state = self.state.lock().unwrap();
                            if let Some(fc) = FunctionCall::from(&fc_arguments_done.name, &fc_arguments_done.arguments) {
                                // ignore function call results
                                match state.executor.execute(&fc) {
                                    Ok(_) => {},
                                    Err(err) => {
                                        gst::error!(CAT, imp = self, "Error sending function call: {:?}", err);
                                        // 重置执行器连接状态
                                        state.executor.reset_connection();
                                        
                                        // 尝试重新连接
                                        let parent = self
                                            .obj()
                                            .parent()
                                            .and_downcast::<super::RealtimeTransformer>()
                                            .expect("has parent");
                                        
                                        let settings = match parent.imp().settings.lock() {
                                            Ok(settings) => settings,
                                            Err(e) => {
                                                gst::error!(CAT, imp = self, "Failed to get settings: {}", e);
                                                return Ok(());
                                            }
                                        };
                                        
                                        if let Err(conn_err) = state.executor.ensure_connection(&settings.executor_addr) {
                                            gst::error!(CAT, imp = self, "Failed to reconnect to executor: {}", conn_err);
                                        }
                                    }
                                };
                            } else {
                                gst::error!(CAT, imp = self, "Error parsing function call: {:?}", fc_arguments_done);
                            }
                        },
                        server_event::ServerEvent::InputAudioBufferCommited(_) => {
                            let parent = self
                                .obj()
                                .parent()
                                .and_downcast::<super::RealtimeTransformer>()
                                .expect("has parent");
                            {
                                let mut state = parent.imp().state.lock().unwrap();
                                // reset input audio seq no
                                state.acc_audio_duration = gst::ClockTime::from_seconds(0);
                            }
                            {
                                let mut sstate = self.state.lock().unwrap();
                                sstate.reset_buffers();
                            }
                        },
                        server_event::ServerEvent::SessionUpdated(_) => {},
                        server_event::ServerEvent::ConversationItemCreated(_) => {},
                        server_event::ServerEvent::ResponseDone(_) => {},
                        server_event::ServerEvent::RateLimitsUpdated(_) => {},
                        server_event::ServerEvent::ResponseContentPartAdded(_) => {},
                        server_event::ServerEvent::ResponseAudioTranscriptDelta(_) => {},
                        server_event::ServerEvent::ConversationItemInputAudioTranscriptionCompleted(_) => {},
                        server_event::ServerEvent::ResponseAudioDone(_) => {},
                        server_event::ServerEvent::ResponseAudioTranscriptDone(_) => {},
                        server_event::ServerEvent::ResponseContentPartDone(_) => {},
                        server_event::ServerEvent::ResponseOutputItemDone(_) => {},
                        server_event::ServerEvent::InputAudioBufferSpeechStarted(_) => {},
                        server_event::ServerEvent::ConversationCreated(_) => {},
                        server_event::ServerEvent::ResponseVideoDone(video_done) => {
                            gst::info!(
                                CAT,
                                imp = self,
                                "Response video done, event_id: {}, response_id: {}, face_info: {:?}",
                                video_done.event_id,
                                video_done.response_id,
                                video_done.face_info
                            );
                        },
                        _ => {
                            gst::warning!(CAT, "Unhandled message type: {}", msg);
                        },
                    }

                    Ok(())
                },
                _ => Ok(()),
            }
        };

        /* Wrap in a timeout so we can push gaps regularly */
        let future = async move {
            match tokio::time::timeout(Duration::from_millis(GRANULARITY_MS.into()), future).await {
                Err(_) => {
                    if !self.dequeue() {
                        gst::info!(CAT, imp = self, "Failed to dequeue, pausing");

                        let _ = self.obj().pause_task();
                    }
                    Ok(())
                },
                Ok(res) => {
                    if !self.dequeue() {
                        gst::info!(CAT, imp = self, "Failed to dequeue, pausing");

                        let _ = self.obj().pause_task();
                    }
                    res
                },
            }
        };

        RUNTIME.block_on(future)
    }

    fn start_task(&self) -> Result<(), gst::LoggableError> {
        let this_weak = self.downgrade();
        let pad_weak = self.obj().downgrade();
        let (sender, mut receiver) = mpsc::channel(1);

        self.state.lock().unwrap().sender = Some(sender);

        let res = self.obj().start_task(move || {
            let Some(this) = this_weak.upgrade() else {
                if let Some(pad) = pad_weak.upgrade() {
                    let _ = pad.pause_task();
                }
                return;
            };

            if let Err(err) = this.loop_fn(&mut receiver) {
                let parent = this
                    .obj()
                    .parent()
                    .and_downcast::<gst::Element>()
                    .expect("has parent");
                gst::element_error!(
                    parent,
                    gst::StreamError::Failed,
                    ["Streaming failed: {}", err]
                );
                let _ = this.obj().pause_task();
            }
        });
        if res.is_err() {
            return Err(gst::loggable_error!(CAT, "Failed to start pad task"));
        }
        Ok(())
    }

    fn stop_task(&self) -> Result<(), glib::BoolError> {
        self.state.lock().unwrap().sender = None;

        self.obj().stop_task()
    }
}

impl RealtimeTransformer {
    fn src_activatemode(
        &self,
        pad: &super::RealtimeSrcPad,
        _mode: gst::PadMode,
        active: bool,
    ) -> Result<(), gst::LoggableError> {
        if active {
            pad.imp().start_task()?;
        } else {
            pad.imp().stop_task()?;
        }

        Ok(())
    }

    fn src_query(&self, pad: &super::RealtimeSrcPad, query: &mut gst::QueryRef) -> bool {
        gst::log!(CAT, obj = pad, "Handling query {:?}", query);

        match query.view_mut() {
            gst::QueryViewMut::Position(ref mut q) => {
                if q.format() == gst::Format::Time {
                    let sstate = pad.imp().state.lock().unwrap();
                    q.set(
                        sstate
                            .out_segment
                            .to_running_time(sstate.out_segment.position()),
                    );
                    true
                } else {
                    false
                }
            },
            _ => gst::Pad::query_default(pad, Some(&*self.obj()), query),
        }
    }

    fn sink_event(&self, pad: &gst::Pad, event: gst::Event) -> bool {
        gst::debug!(CAT, obj = pad, "Handling event {:?}", event);

        match event.view() {
            gst::EventView::Eos(_) => match self.handle_buffer(pad, None) {
                Err(err) => {
                    gst::error!(CAT, "Failed to send EOS: {}", err);
                    false
                },
                Ok(_) => true,
            },
            gst::EventView::FlushStart(_) => {
                gst::info!(CAT, imp = self, "Received flush start, disconnecting");
                match self.disconnect() {
                    Err(err) => {
                        self.post_error_message(err);
                        false
                    },
                    Ok(_) => {
                        let mut ret = gst::Pad::event_default(pad, Some(&*self.obj()), event);

                        let state = self.state.lock().unwrap();
                        for srcpad in &state.srcpads {
                            if let Err(err) = srcpad.imp().stop_task() {
                                gst::error!(CAT, imp = self, "Failed to stop srcpad task: {}", err);
                                ret = false;
                            }
                        }

                        ret
                    },
                }
            },
            gst::EventView::FlushStop(_) => {
                gst::info!(CAT, imp = self, "Received flush stop, restarting task");

                if gst::Pad::event_default(pad, Some(&*self.obj()), event) {
                    let state = self.state.lock().unwrap();
                    for srcpad in &state.srcpads {
                        if let Err(err) = srcpad.imp().start_task() {
                            gst::error!(CAT, imp = self, "Failed to start srcpad task: {}", err);
                            return false;
                        }
                    }
                    true
                } else {
                    false
                }
            },
            gst::EventView::Segment(e) => {
                let segment = match e.segment().clone().downcast::<gst::ClockTime>() {
                    Err(segment) => {
                        gst::element_imp_error!(
                            self,
                            gst::StreamError::Format,
                            ["Only Time segments supported, got {:?}", segment.format(),]
                        );
                        return false;
                    },
                    Ok(segment) => segment,
                };

                let mut state = self.state.lock().unwrap();

                for srcpad in &state.srcpads {
                    let mut sstate = srcpad.imp().state.lock().unwrap();
                    sstate.out_segment.set_time(segment.time());
                    sstate.out_segment.set_position(gst::ClockTime::ZERO);
                    sstate.seqnum = e.seqnum();
                    srcpad.sticky_events_foreach(|e| {
                        if let gst::EventView::Segment(_) = e.view() {
                            std::ops::ControlFlow::Continue(gst::EventForeachAction::Remove)
                        } else {
                            std::ops::ControlFlow::Continue(gst::EventForeachAction::Keep)
                        }
                    });
                }

                state.in_segment = segment;

                true
            },
            gst::EventView::Tag(_) => true,
            gst::EventView::Caps(_) => true,
            _ => gst::Pad::event_default(pad, Some(&*self.obj()), event),
        }
    }

    async fn sync_and_send(
        &self,
        buffer: Option<gst::Buffer>,
    ) -> Result<gst::FlowSuccess, gst::FlowError> {
        let mut delay = None;

        {
            let state = self.state.lock().unwrap();

            if let Some(ref buffer) = buffer {
                let running_time = state
                    .in_segment
                    .to_running_time(buffer.pts().expect("Checked in sink_chain()"));
                let now = self.obj().current_running_time().unwrap();

                if let Some(running_time) = running_time {
                    delay = running_time.checked_sub(now);
                }
            }
        }

        if let Some(delay) = delay {
            tokio::time::sleep(Duration::from_nanos(delay.nseconds())).await;
        }

        let mut buffer_duration = gst::ClockTime::from_seconds(0);

        // 使用作用域来限制借用
        {
            let mut ws_sink_ref = self.ws_sink.borrow_mut();
            let ws_sink = match ws_sink_ref.as_mut() {
                Some(sink) => sink,
                None => {
                    gst::error!(CAT, imp = self, "No WebSocket connection available");
                    return Err(gst::FlowError::Error);
                }
            };

            if let Some(buffer) = buffer {
                let data = buffer.map_readable().unwrap();
                gst::debug!(CAT, "Sent {} bytes via websocket", data.len());

                let encoded_bytes = BASE64_STANDARD.encode(data);
                let audio_append_message = client_event::InputAudioBufferAppend {
                    event_id: None,
                    audio: encoded_bytes,
                };
                match ws_sink.send(Message::from(audio_append_message)).await {
                    Ok(_) => {
                        buffer_duration += buffer.duration().unwrap_or(gst::ClockTime::from_mseconds(
                            calculate_audio_length_ms(buffer.size()),
                        ));
                    },
                    Err(err) => {
                        gst::error!(CAT, imp = self, "Failed sending packet: {}", err);
                        
                        // 重置连接状态以便下次调用ensure_connection时重新连接
                        drop(ws_sink_ref); // 先释放借用
                        let mut state = self.state.lock().unwrap();
                        state.connected = false;
                        // 清空WebSocket连接
                        *self.ws_sink.borrow_mut() = None;
                        
                        return Err(gst::FlowError::Error);
                    }
                };
            } else {
                gst::warning!(
                    CAT,
                    "no audio in the sink pad when handle buffer, this should NOT happen"
                );
            }
        }

        {
            let mut state = self.state.lock().unwrap();
            state.acc_audio_duration += buffer_duration;
            // cancel previous timeout handler
            if let Some(abort_handle) = state.vad_abort_handle.take() {
                abort_handle.abort();
            }
        }

        // detect vad ending
        if !self.settings.lock().unwrap().server_vad
            && self.state.lock().unwrap().acc_audio_duration
                > self.settings.lock().unwrap().vad_min_audio_duration
        {
            let this_weak = self.downgrade();
            let silience_duration = self.settings.lock().unwrap().vad_silence_duration_ms as u64;

            let audio_duration = self.state.lock().unwrap().acc_audio_duration;
            let future = async move {
                tokio::time::sleep(Duration::from_millis(silience_duration)).await;

                let ret: Result<(), gst::ErrorMessage> = Ok(());
                if let Some(this) = this_weak.upgrade() {
                    let mut ws_sink_ref = this.ws_sink.borrow_mut();
                    if let Some(ws_sink) = ws_sink_ref.as_mut() {
                        gst::debug!(CAT, "Commit audio buffer {} ", audio_duration);
                        let audio_buffer_commit =
                            client_event::InputAudioBufferCommit { event_id: None };

                        if let Err(err) = ws_sink.send(Message::from(audio_buffer_commit)).await {
                            gst::error!(CAT, "Failed to send InputAudioBufferCommit: {}", err);
                            // 重置连接状态以便下次调用ensure_connection时重新连接
                            drop(ws_sink_ref); // 先释放借用
                            if let Ok(mut state) = this.state.lock() {
                                state.connected = false;
                            }
                            // 清空WebSocket连接
                            *this.ws_sink.borrow_mut() = None;
                            
                            return Ok(());
                        }

                        let response_create = client_event::ResponseCreate { event_id: None };
                        if let Err(err) = ws_sink.send(Message::from(response_create)).await {
                            gst::error!(CAT, "Failed to send ResponseCreate: {}", err);
                            // 重置连接状态以便下次调用ensure_connection时重新连接
                            drop(ws_sink_ref); // 先释放借用
                            if let Ok(mut state) = this.state.lock() {
                                state.connected = false;
                            }
                            // 清空WebSocket连接
                            *this.ws_sink.borrow_mut() = None;
                            
                            return Ok(());
                        }
                    }
                }
                ret
            };
            let (future, abort_handle) = abortable(future);
            self.state.lock().unwrap().vad_abort_handle = Some(abort_handle);
            RUNTIME.spawn(future);
        }

        Ok(gst::FlowSuccess::Ok)
    }

    fn handle_buffer(
        &self,
        _pad: &gst::Pad,
        buffer: Option<gst::Buffer>,
    ) -> Result<gst::FlowSuccess, gst::FlowError> {
        gst::trace!(CAT, imp = self, "Handling {:?}", buffer);

        self.ensure_connection().map_err(|err| {
            // No need to worry too much here, we didn't have a session to
            // terminate in the first place
            if buffer.is_none() {
                return gst::FlowError::Eos;
            }
            gst::element_imp_error!(
                self,
                gst::StreamError::Failed,
                ["Streaming failed: {}", err]
            );
            gst::FlowError::Error
        })?;
        
        // 创建一个单次使用的channel来获取结果
        let (tx, rx) = tokio::sync::oneshot::channel();
        
        // 克隆需要的数据
        let buffer_clone = buffer.clone();
        let this_weak = self.downgrade();
        
        // 创建Future，使用weak引用而不是直接捕获self
        let future = async move {
            if let Some(this) = this_weak.upgrade() {
                match this.sync_and_send(buffer_clone).await {
                    Ok(res) => {
                        let _ = tx.send(Ok(res));
                    },
                    Err(err) => {
                        let _ = tx.send(Err(err));
                    }
                }
            } else {
                let _ = tx.send(Err(gst::FlowError::Error));
            }
        };
        
        let (future, abort_handle) = abortable(future);
        self.state.lock().unwrap().send_abort_handle = Some(abort_handle);
        
        // 启动异步任务
        RUNTIME.spawn(future);
        
        // 阻塞等待结果
        match rx.blocking_recv() {
            Ok(result) => result,
            Err(_) => {
                gst::error!(CAT, "Failed to receive result from async task");
                Err(gst::FlowError::Error)
            }
        }
    }

    fn sink_chain(
        &self,
        pad: &gst::Pad,
        buffer: gst::Buffer,
    ) -> Result<gst::FlowSuccess, gst::FlowError> {
        if buffer.pts().is_none() {
            gst::error!(CAT, imp = self, "Only buffers with PTS supported");
            return Err(gst::FlowError::Error);
        }

        self.handle_buffer(pad, Some(buffer))
    }

    fn ensure_connection(&self) -> Result<(), gst::ErrorMessage> {
        // 安全地获取锁
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(err) => {
                return Err(gst::error_msg!(
                    gst::CoreError::Failed,
                    ["Failed to acquire state lock: {}", err]
                ));
            }
        };
        
        let settings = match self.settings.lock() {
            Ok(settings) => settings,
            Err(err) => {
                return Err(gst::error_msg!(
                    gst::CoreError::Failed,
                    ["Failed to acquire settings lock: {}", err]
                ));
            }
        };

        if state.connected {
            return Ok(());
        }

        gst::info!(CAT, imp = self, "Connecting ..");

        let url = match &settings.url {
            Some(url) => url.to_string(),
            None => "ws://0.0.0.0:9000".to_string(),
        };

        let uri = Url::parse(&url).map_err(|e| {
            gst::error_msg!(
                gst::CoreError::Failed,
                ["Failed to parse provided url: {}", e]
            )
        })?;
        
        let Some(api_key) = settings.api_key.clone() else {
            return Err(gst::error_msg!(
                gst::CoreError::Failed,
                ["An API key is required"]
            ));
        };
        
        let authority = uri.authority();
        let host = authority.splitn(2, '@').last().unwrap_or("");

        // 构建WebSocket请求
        let request = Request::builder()
            .method("GET")
            .uri(&url)
            .header("Host", host)
            .header("Upgrade", "websocket")
            .header("Connection", "keep-alive, upgrade")
            .header(
                "Sec-Websocket-Key",
                async_tungstenite::tungstenite::handshake::client::generate_key(),
            )
            .header("Sec-Websocket-Version", "13")
            .header("Authorization", format!("Bearer {}", &api_key));

        // 如果有agent_id，添加到请求头
        let request = if let Some(agent_id) = &settings.agent_id {
            request.header("X-Ticos-Agent-ID", agent_id)
        } else {
            request
        };

        let request = request
            .body(())
            .unwrap();

        // 创建一个oneshot通道来处理连接结果
        let (tx, rx) = tokio::sync::oneshot::channel();
        let tx = std::sync::Arc::new(tokio::sync::Mutex::new(Some(tx)));
        
        // 克隆需要的设置
        let max_attempts = settings.reconnect_attempts;
        let reconnect_delay_ms = settings.reconnect_delay_ms;
        let connection_timeout_ms = settings.connection_timeout_ms;
        let agent_id = settings.agent_id.clone();
        let model_name = settings.model_name.clone();
        let tools = settings.tools.clone();
        let model_provider = settings.model_provider.clone();
        let instructions = settings.instructions.clone();
        let server_vad = settings.server_vad;
        let vad_threshold = settings.vad_threshold;
        let vad_silence_duration_ms = settings.vad_silence_duration_ms;
        let voice = settings.voice.clone();
        let knowledge_scripts = settings.knowledge_scripts.clone();
        
        // 在 ensure_connection 函数中，在创建异步任务之前获取所需的值
        let vision_config = if settings.vision_enable_face_detection 
            || settings.vision_enable_face_identification 
            || settings.vision_enable_object_detection {
            Some(Vision {
                enable_face_detection: settings.vision_enable_face_detection,
                enable_face_identification: settings.vision_enable_face_identification,
                enable_object_detection: settings.vision_enable_object_detection,
                object_detection_target_classes: settings.vision_object_detection_classes.clone(),
            })
        } else {
            None
        };

        // 释放settings锁
        drop(settings);
        
        // 启动异步连接任务
        RUNTIME.spawn(async move {
            let mut attempts = 0;
            let mut last_error = None;
            let mut connection = None;

            while attempts < max_attempts {
                gst::debug!(
                    CAT,
                    "WebSocket connection attempt {}/{}",
                    attempts + 1,
                    max_attempts
                );

                let connect_future = connect_async(request.clone());
                match tokio::time::timeout(
                    Duration::from_millis(connection_timeout_ms),
                    connect_future
                ).await {
                    Ok(Ok((ws_stream, _))) => {
                        connection = Some(ws_stream);
                        break;
                    },
                    Ok(Err(err)) => {
                        gst::warning!(
                            CAT,
                            "Connection attempt {} failed: {}, retrying in {}ms...",
                            attempts + 1,
                            err,
                            reconnect_delay_ms
                        );
                        last_error = Some(format!("WebSocket connection error: {}", err));
                    },
                    Err(timeout_err) => {
                        gst::warning!(
                            CAT,
                            "Connection attempt {} timed out after {}ms: {}, retrying in {}ms...",
                            attempts + 1,
                            connection_timeout_ms,
                            timeout_err,
                            reconnect_delay_ms
                        );
                        last_error = Some(format!("Connection timeout: {}", timeout_err));
                    }
                }

                attempts += 1;
                
                if attempts < max_attempts {
                    tokio::time::sleep(Duration::from_millis(reconnect_delay_ms)).await;
                }
            }

            // 处理连接结果
            match connection {
                Some(mut ws_stream) => {
                    // 发送初始化消息
                    let start_message = Message::from(client_event::SessionUpdate {
                        event_id: None,
                        session: Session {
                            agent_id,
                            model: if model_name.is_some() || !tools.is_none() || model_provider.is_some() || instructions.is_some() {
                                Some(Model {
                                    name: model_name,
                                    tools,
                                    provider: model_provider,
                                    modalities: Some(vec!["text".into(), "audio".into()]),
                                    temperature: Some(0.7),
                                    instructions,
                                    max_response_output_tokens: Some(MaxOutputTokens::Num(4096)),
                                })
                            } else {
                                None
                            },
                            hearing: if server_vad || true /* input_audio_format 始终设置 */ {
                                Some(Hearing {
                                    turn_detection: if server_vad {
                                        Some(TurnDetection::ServerVAD {
                                            threshold: vad_threshold,
                                            prefix_padding_ms: DEFAULT_VAD_PREFIX_PADDING_MS,
                                            silence_duration_ms: vad_silence_duration_ms,
                                        })
                                    } else {
                                        None
                                    },
                                    input_audio_format: Some(AudioFormat::PCM16),
                                })
                            } else {
                                None
                            },
                            speech: if voice.is_some() || true /* output_audio_format 始终设置 */ {
                                Some(Speech {
                                    voice,
                                    output_audio_format: Some(AudioFormat::PCM16),
                                })
                            } else {
                                None
                            },
                            knowledge: if !knowledge_scripts.is_empty() {
                                Some(Knowledge {
                                    scripts: knowledge_scripts
                                        .iter()
                                        .map(|x| KnowledgeScript { id: x.clone() })
                                        .collect(),
                                })
                            } else {
                                None
                            },
                            vision: vision_config,
                        },
                    });

                    println!("start_message: {:?}", start_message);

                    if let Err(err) = ws_stream.send(start_message).await {
                        if let Some(sender) = tx.lock().await.take() {
                            let _ = sender.send(Err(format!("Failed to send initial message: {}", err)));
                        }
                        return;
                    }

                    let tx_clone = tx.clone();
                    // 等待会话创建确认
                    let timeout = tokio::time::timeout(
                        Duration::from_millis(connection_timeout_ms),
                        async {
                            let mut success = false;
                            let (write, mut read) = ws_stream.split();
                            while let Some(message_result) = read.next().await {
                                match message_result {
                                    Ok(msg) => {
                                        if let Message::Text(text) = msg {
                                            if let Ok(event) = serde_json::from_slice::<server_event::ServerEvent>(text.as_bytes()) {
                                                match event {
                                                    server_event::ServerEvent::SessionCreated(_) |
                                                    server_event::ServerEvent::SessionUpdated(_) => {
                                                        success = true;
                                                        break;
                                                    },
                                                    server_event::ServerEvent::Error(e) => {
                                                        if let Some(sender) = tx_clone.lock().await.take() {
                                                            let _ = sender.send(Err(format!("Session update failed: {} ({})", e.error.r#type, e.error.message)));
                                                        }
                                                        return;
                                                    },
                                                    _ => continue,
                                                }
                                            }
                                        }
                                    },
                                    Err(err) => {
                                        if let Some(sender) = tx_clone.lock().await.take() {
                                            let _ = sender.send(Err(format!("Failed to receive message: {}", err)));
                                        }
                                        return;
                                    }
                                }
                            }
                            if success {
                                if let Some(sender) = tx_clone.lock().await.take() {
                                    match write.reunite(read) {
                                        Ok(stream) => {
                                            let _ = sender.send(Ok(stream));
                                        },
                                        Err(_) => {
                                            let _ = sender.send(Err("Failed to reunite WebSocket stream".to_string()));
                                        }
                                    }
                                }
                            } else {
                                if let Some(sender) = tx_clone.lock().await.take() {
                                    let _ = sender.send(Err("No session created confirmation received".to_string()));
                                }
                            }
                        }
                    ).await;

                    if let Err(_) = timeout {
                        if let Some(sender) = tx.lock().await.take() {
                            let _ = sender.send(Err("Timeout waiting for session creation confirmation".to_string()));
                        }
                    }
                },
                None => {
                    if let Some(sender) = tx.lock().await.take() {
                        let _ = sender.send(Err(last_error.unwrap_or_else(|| {
                            format!("Failed to connect after {} attempts", max_attempts)
                        })));
                    }
                }
            }
        });

        // 等待连接结果
        match rx.blocking_recv() {
            Ok(Ok(ws_stream)) => {
                // 存储WebSocket连接并启动接收消息的任务
                let (write_half, mut read_half) = ws_stream.split();
                *self.ws_sink.borrow_mut() = Some(Box::pin(write_half));
                
                // 设置连接状态
                state.connected = true;

                // 播放提示音
                let sine_wave = create_sine_wave(500);
                let mut buf = gst::Buffer::from_mut_slice(sine_wave);
                {
                    let buf = buf.get_mut().unwrap();
                    buf.set_pts(gst::ClockTime::ZERO);
                    buf.set_duration(gst::ClockTime::from_mseconds(100));
                }
                for srcpad in &state.srcpads {
                    let mut sstate = srcpad.imp().state.lock().unwrap();
                    sstate.push_buffer(buf.clone());
                }
                
                // 启动接收消息的任务
                let this_weak = self.downgrade();
                let (future, abort_handle) = abortable(async move {
                    while let Some(msg_result) = read_half.next().await {
                        if let Some(this) = this_weak.upgrade() {
                            if let Ok(msg) = msg_result {
                                // 处理接收到的消息
                                let srcpads = {
                                    let state = this.state.lock().unwrap();
                                    state.srcpads.clone()
                                };
                                for srcpad in srcpads {
                                    let sender = {
                                        let state = srcpad.imp().state.lock().unwrap();
                                        state.sender.clone()
                                    };
                                    if let Some(mut sender) = sender {
                                        if sender.send(msg.clone()).await.is_err() {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                });
                
                state.recv_abort_handle = Some(abort_handle);
                RUNTIME.spawn(future);
                
                gst::info!(CAT, imp = self, "Connected successfully");
                Ok(())
            },
            Ok(Err(err)) => {
                Err(gst::error_msg!(
                    gst::CoreError::Failed,
                    ["Connection failed: {}", err]
                ))
            },
            Err(_) => {
                Err(gst::error_msg!(
                    gst::CoreError::Failed,
                    ["Connection task failed"]
                ))
            }
        }
    }

    fn disconnect(&self) -> Result<(), gst::ErrorMessage> {
        // 安全地获取state锁
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(err) => {
                return Err(gst::error_msg!(
                    gst::CoreError::Failed,
                    ["Failed to acquire state lock: {}", err]
                ));
            }
        };

        gst::info!(CAT, imp = self, "Unpreparing");

        if let Some(abort_handle) = state.recv_abort_handle.take() {
            abort_handle.abort();
        }

        if let Some(abort_handle) = state.send_abort_handle.take() {
            abort_handle.abort();
        }

        let _ = self.sinkpad.stream_lock();

        // 获取WebSocket并转移所有权
        let ws_sink = self.ws_sink.borrow_mut().take();
        if let Some(mut ws_sink) = ws_sink {
            // 使用oneshot通道来处理WebSocket关闭
            let (tx, rx) = tokio::sync::oneshot::channel();
            
            RUNTIME.spawn(async move {
                let _ = ws_sink.close().await;
                let _ = tx.send(());
            });
            
            // 使用超时机制等待关闭完成
            match rx.blocking_recv() {
                Ok(_) => {
                    gst::debug!(CAT, imp = self, "WebSocket closed successfully");
                },
                Err(err) => {
                    gst::warning!(CAT, imp = self, "Failed to close WebSocket: {}", err);
                }
            }
        }

        *state = State::default();

        gst::info!(
            CAT,
            imp = self,
            "Unprepared, connected: {}!",
            state.connected
        );

        Ok(())
    }
}

// Implementation of gst::ChildProxy virtual methods.
//
// This allows accessing the pads and their properties from e.g. gst-launch.
impl ChildProxyImpl for RealtimeTransformer {
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

#[glib::object_subclass]
impl ObjectSubclass for RealtimeTransformer {
    const NAME: &'static str = "GstOpenaiRealtime";
    type Type = super::RealtimeTransformer;
    type ParentType = gst::Element;
    type Interfaces = (gst::ChildProxy,);

    fn with_class(klass: &Self::Class) -> Self {
        let templ = klass.pad_template("sink").unwrap();
        let sinkpad = gst::Pad::builder_from_template(&templ)
            .chain_function(|pad, parent, buffer| {
                RealtimeTransformer::catch_panic_pad_function(
                    parent,
                    || Err(gst::FlowError::Error),
                    |transcriber| transcriber.sink_chain(pad, buffer),
                )
            })
            .event_function(|pad, parent, event| {
                RealtimeTransformer::catch_panic_pad_function(
                    parent,
                    || false,
                    |transcriber| transcriber.sink_event(pad, event),
                )
            })
            .build();

        let settings = Mutex::new(Settings::default());

        Self {
            sinkpad,
            settings,
            state: Default::default(),
            ws_sink: Default::default(),
        }
    }
}

impl GstObjectImpl for RealtimeTransformer {}

impl ObjectImpl for RealtimeTransformer {
    fn properties() -> &'static [glib::ParamSpec] {
        static PROPERTIES: LazyLock<Vec<glib::ParamSpec>> = LazyLock::new(|| {
            vec![
                glib::ParamSpecString::builder("url")
                    .nick("URL")
                    .blurb("URL of the OpenAI Realtime server")
                    .default_value("ws://0.0.0.0:9000")
                    .build(),
                glib::ParamSpecString::builder("api-key")
                    .nick("API Key")
                    .blurb("OpenAI API Key")
                    .mutable_ready()
                    .build(),
                glib::ParamSpecString::builder("agent-id")
                    .nick("Agent ID")
                    .blurb("Agent ID")
                    .mutable_ready()
                    .build(),
                glib::ParamSpecUInt::builder("buffer-time")
                    .nick("Buffer Time")
                    .blurb("Amount of milliseconds audio to buffer")
                    .default_value(DEFAULT_BUFFER_TIME_MS)
                    .build(),
                glib::ParamSpecString::builder("voice")
                    .nick("Speech Voice")
                    .blurb("Speech Voice")
                    .build(),
                glib::ParamSpecString::builder("model-name")
                    .nick("Model Name")
                    .blurb("LLM model used to generate outputs")
                    .build(),
                glib::ParamSpecString::builder("model-provider")
                    .nick("Model Provider")
                    .blurb("The one who trained the LLM model")
                    .build(),
                glib::ParamSpecString::builder("instructions")
                    .nick("Model Instructions")
                    .blurb("The instructions to model")
                    .build(),
                glib::ParamSpecBoolean::builder("server-vad")
                    .nick("Server VAD")
                    .blurb("Enable/disable server vad")
                    .default_value(false)
                    .build(),
                glib::ParamSpecFloat::builder("vad-threshold")
                    .nick("VAD Threshold")
                    .blurb("Threshold of server VAD")
                    .minimum(0 as f32)
                    .maximum(1 as f32)
                    .default_value(DEFAULT_VAD_THRESHOLD)
                    .build(),
                glib::ParamSpecUInt::builder("vad-silence-duration")
                    .nick("VAD Silence Duration")
                    .blurb("Amount of milliseconds of silence to end a turn")
                    .default_value(DEFAULT_VAD_SILENCE_DURATION_MS)
                    .build(),
                glib::ParamSpecUInt::builder("vad-min-audio-duration")
                    .nick("VAD Min Audio Duration")
                    .blurb("Minimum milliseconds of audio to commit")
                    .default_value(DEFAULT_VAD_MIN_AUDIO_DURATION_MS)
                    .build(),
                glib::ParamSpecString::builder("executor-addr")
                    .nick("Executor TCP Address")
                    .blurb("TCP address of executor, such as 127.0.0.1:9999")
                    .build(),
                glib::ParamSpecString::builder("knowledge-scripts")
                    .nick("Knowledge Scripts")
                    .blurb("Knowledge scripts to run, if there are multiple, separate with ','")
                    .build(),
                glib::ParamSpecString::builder("tools")
                    .nick("Tools")
                    .blurb("JSON string of tools configuration")
                    .build(),
                glib::ParamSpecUInt::builder("reconnect-attempts")
                    .nick("Reconnect Attempts")
                    .blurb("Maximum number of reconnection attempts")
                    .default_value(DEFAULT_RECONNECT_ATTEMPTS)
                    .build(),
                glib::ParamSpecUInt64::builder("reconnect-delay-ms")
                    .nick("Reconnect Delay")
                    .blurb("Delay between reconnection attempts in milliseconds")
                    .default_value(DEFAULT_RECONNECT_DELAY_MS)
                    .build(),
                glib::ParamSpecUInt64::builder("connection-timeout-ms")
                    .nick("Connection Timeout")
                    .blurb("Connection timeout in milliseconds")
                    .default_value(DEFAULT_CONNECTION_TIMEOUT_MS)
                    .build(),
                glib::ParamSpecBoolean::builder("vision-enable-face-detection")
                    .nick("Enable Face Detection")
                    .blurb("Enable face detection in vision processing")
                    .default_value(false)
                    .build(),
                glib::ParamSpecBoolean::builder("vision-enable-face-identification")
                    .nick("Enable Face Identification")
                    .blurb("Enable face identification in vision processing")
                    .default_value(false)
                    .build(),
                glib::ParamSpecBoolean::builder("vision-enable-object-detection")
                    .nick("Enable Object Detection")
                    .blurb("Enable object detection in vision processing")
                    .default_value(false)
                    .build(),
                glib::ParamSpecString::builder("vision-object-detection-classes")
                    .nick("Object Detection Classes")
                    .blurb("Target classes for object detection, comma-separated list")
                    .build(),
            ]
        });

        PROPERTIES.as_ref()
    }

    fn constructed(&self) {
        self.parent_constructed();

        let obj = self.obj();
        obj.add_pad(&self.sinkpad).unwrap();

        let templ = obj.class().pad_template("src").unwrap();
        let srcpad: super::RealtimeSrcPad = gst::PadBuilder::from_template(&templ)
            .activatemode_function(|pad, parent, mode, active| {
                RealtimeTransformer::catch_panic_pad_function(
                    parent,
                    || {
                        Err(gst::loggable_error!(
                            CAT,
                            "Panic activating src pad with mode"
                        ))
                    },
                    |transformer| transformer.src_activatemode(pad, mode, active),
                )
            })
            .query_function(|pad, parent, query| {
                RealtimeTransformer::catch_panic_pad_function(
                    parent,
                    || false,
                    |transformer| transformer.src_query(pad, query),
                )
            })
            .flags(gst::PadFlags::FIXED_CAPS)
            .build();
        obj.add_pad(&srcpad).unwrap();
        self.state.lock().unwrap().srcpads.insert(srcpad);
        obj.set_element_flags(gst::ElementFlags::PROVIDE_CLOCK | gst::ElementFlags::REQUIRE_CLOCK);
    }

    fn set_property(&self, _id: usize, value: &glib::Value, pspec: &glib::ParamSpec) {
        match pspec.name() {
            "url" => {
                let mut settings = self.settings.lock().unwrap();
                settings.url = value.get().expect("type checked upstream");
            },
            "api-key" => {
                let mut settings = self.settings.lock().unwrap();
                settings.api_key = value.get().expect("type checked upstream");
            },
            "agent-id" => {
                let mut settings = self.settings.lock().unwrap();
                settings.agent_id = value.get().expect("type checked upstream");
            },
            "buffer-time" => {
                let mut settings = self.settings.lock().unwrap();
                settings.buffer_time_ms = value.get().expect("type checked upstream");
            },
            "voice" => {
                let mut settings = self.settings.lock().unwrap();
                let value_str: Option<String> = value.get().expect("type checked upstream");
                settings.voice = value_str;
            },
            "model-name" => {
                let mut settings = self.settings.lock().unwrap();
                let value_str: Option<String> = value.get().expect("type checked upstream");
                settings.model_name = value_str;
            },
            "model-provider" => {
                let mut settings = self.settings.lock().unwrap();
                let value_str: Option<String> = value.get().expect("type checked upstream");
                settings.model_provider = value_str;
            },
            "instructions" => {
                let mut settings = self.settings.lock().unwrap();
                let value_str: Option<String> = value.get().expect("type checked upstream");
                settings.instructions = value_str;
            },
            "server-vad" => {
                let mut settings = self.settings.lock().unwrap();
                settings.server_vad = value.get().expect("type checked upstream");
            },
            "vad-threshold" => {
                let mut settings = self.settings.lock().unwrap();
                settings.vad_threshold = value.get().expect("type checked upstream");
            },
            "vad-silence-duration" => {
                let mut settings = self.settings.lock().unwrap();
                settings.vad_silence_duration_ms = value.get().expect("type checked upstream");
            },
            "vad-min-audio-duration" => {
                let mut settings = self.settings.lock().unwrap();
                settings.vad_min_audio_duration = value
                    .get::<u32>()
                    .map(|x| gst::ClockTime::from_mseconds(x as u64))
                    .expect("type checked upstream");
            },
            "executor-addr" => {
                let mut settings = self.settings.lock().unwrap();
                settings.executor_addr = value.get().expect("type checked upstream");
            },
            "knowledge-scripts" => {
                let mut settings = self.settings.lock().unwrap();
                settings.knowledge_scripts = value
                    .get::<String>()
                    .map(|x| x.split(',').map(|x| x.trim()).map(String::from).collect())
                    .expect("type checked upstream");
            },
            "tools" => {
                let mut settings = self.settings.lock().unwrap();
                let value_str = value.get::<String>().expect("type checked upstream");
                if let Ok(tools) = serde_json::from_str::<Vec<ToolDefinition>>(&value_str) {
                    settings.tools = Some(tools);
                } else {
                    gst::warning!(CAT, "Failed to parse tools JSON: {}", value_str);
                }
            },
            "reconnect-attempts" => {
                let mut settings = self.settings.lock().unwrap();
                settings.reconnect_attempts = value.get().expect("type checked upstream");
            },
            "reconnect-delay-ms" => {
                let mut settings = self.settings.lock().unwrap();
                settings.reconnect_delay_ms = value.get().expect("type checked upstream");
            },
            "connection-timeout-ms" => {
                let mut settings = self.settings.lock().unwrap();
                settings.connection_timeout_ms = value.get().expect("type checked upstream");
            },
            "vision-enable-face-detection" => {
                let mut settings = self.settings.lock().unwrap();
                settings.vision_enable_face_detection = value.get().expect("type checked upstream");
            },
            "vision-enable-face-identification" => {
                let mut settings = self.settings.lock().unwrap();
                settings.vision_enable_face_identification = value.get().expect("type checked upstream");
            },
            "vision-enable-object-detection" => {
                let mut settings = self.settings.lock().unwrap();
                settings.vision_enable_object_detection = value.get().expect("type checked upstream");
            },
            "vision-object-detection-classes" => {
                let mut settings = self.settings.lock().unwrap();
                settings.vision_object_detection_classes = value
                    .get::<String>()
                    .map(|x| x.split(',').map(|x| x.trim()).map(String::from).collect())
                    .expect("type checked upstream");
            },
            _ => unimplemented!(),
        }
    }

    fn property(&self, _id: usize, pspec: &glib::ParamSpec) -> glib::Value {
        match pspec.name() {
            "url" => {
                let settings = self.settings.lock().unwrap();
                settings.url.to_value()
            },
            "api-key" => {
                let settings = self.settings.lock().unwrap();
                settings.api_key.to_value()
            },
            "agent-id" => {
                let settings = self.settings.lock().unwrap();
                settings.agent_id.to_value()
            },
            "buffer-time" => {
                let settings = self.settings.lock().unwrap();
                settings.buffer_time_ms.to_value()
            },
            "voice" => {
                let settings = self.settings.lock().unwrap();
                settings.voice.to_value()
            },
            "model-name" => {
                let settings = self.settings.lock().unwrap();
                settings.model_name.to_value()
            },
            "model-provider" => {
                let settings = self.settings.lock().unwrap();
                settings.model_provider.to_value()
            },
            "instructions" => {
                let settings = self.settings.lock().unwrap();
                settings.instructions.to_value()
            },
            "server-vad" => {
                let settings = self.settings.lock().unwrap();
                settings.server_vad.to_value()
            },
            "vad-threshold" => {
                let settings = self.settings.lock().unwrap();
                settings.vad_threshold.to_value()
            },
            "vad-silence-duration" => {
                let settings = self.settings.lock().unwrap();
                settings.vad_silence_duration_ms.to_value()
            },
            "vad-min-audio-duration" => {
                let settings = self.settings.lock().unwrap();
                settings.vad_min_audio_duration.mseconds().to_value()
            },
            "executor-addr" => {
                let settings = self.settings.lock().unwrap();
                settings.executor_addr.to_value()
            },
            "knowledge-scripts" => {
                let settings = self.settings.lock().unwrap();
                settings.knowledge_scripts.join(",").to_value()
            },
            "tools" => {
                let settings = self.settings.lock().unwrap();
                settings.tools.as_ref().map(|x| serde_json::to_string(x).unwrap()).unwrap_or_default().to_value()
            },
            "reconnect-attempts" => {
                let settings = self.settings.lock().unwrap();
                settings.reconnect_attempts.to_value()
            },
            "reconnect-delay-ms" => {
                let settings = self.settings.lock().unwrap();
                settings.reconnect_delay_ms.to_value()
            },
            "connection-timeout-ms" => {
                let settings = self.settings.lock().unwrap();
                settings.connection_timeout_ms.to_value()
            },
            "vision-enable-face-detection" => {
                let settings = self.settings.lock().unwrap();
                settings.vision_enable_face_detection.to_value()
            },
            "vision-enable-face-identification" => {
                let settings = self.settings.lock().unwrap();
                settings.vision_enable_face_identification.to_value()
            },
            "vision-enable-object-detection" => {
                let settings = self.settings.lock().unwrap();
                settings.vision_enable_object_detection.to_value()
            },
            "vision-object-detection-classes" => {
                let settings = self.settings.lock().unwrap();
                settings.vision_object_detection_classes.join(",").to_value()
            },
            _ => unimplemented!(),
        }
    }
}

impl ElementImpl for RealtimeTransformer {
    fn metadata() -> Option<&'static gst::subclass::ElementMetadata> {
        static ELEMENT_METADATA: LazyLock<gst::subclass::ElementMetadata> = LazyLock::new(|| {
            gst::subclass::ElementMetadata::new(
                "OpenaiRealtime",
                "Audio/Filter",
                "OpenAI audio to audio filter, using OpenAI Realtime",
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
            let audio_caps = gst_audio::AudioCapsBuilder::new()
                .format(gst_audio::AudioFormat::S16le)
                .rate(SAMPLE_RATE as i32)
                .channels(CHANNELS as i32)
                .layout(gst_audio::AudioLayout::Interleaved)
                .build();
            let src_pad_template = gst::PadTemplate::with_gtype(
                "src",
                gst::PadDirection::Src,
                gst::PadPresence::Always,
                &audio_caps,
                super::RealtimeSrcPad::static_type(),
            )
            .unwrap();
            let req_src_pad_template = gst::PadTemplate::with_gtype(
                "realtime_src_%u",
                gst::PadDirection::Src,
                gst::PadPresence::Request,
                &audio_caps,
                super::RealtimeSrcPad::static_type(),
            )
            .unwrap();

            let sink_pad_template = gst::PadTemplate::new(
                "sink",
                gst::PadDirection::Sink,
                gst::PadPresence::Always,
                &audio_caps,
            )
            .unwrap();

            vec![src_pad_template, req_src_pad_template, sink_pad_template]
        });

        PAD_TEMPLATES.as_ref()
    }

    fn request_new_pad(
        &self,
        templ: &gst::PadTemplate,
        _name: Option<&str>,
        _caps: Option<&gst::Caps>,
    ) -> Option<gst::Pad> {
        let mut state = self.state.lock().unwrap();

        let pad: super::RealtimeSrcPad = gst::PadBuilder::from_template(templ)
            .activatemode_function(|pad, parent, mode, active| {
                RealtimeTransformer::catch_panic_pad_function(
                    parent,
                    || {
                        Err(gst::loggable_error!(
                            CAT,
                            "Panic activating src pad with mode"
                        ))
                    },
                    |transcriber| transcriber.src_activatemode(pad, mode, active),
                )
            })
            .query_function(|pad, parent, query| {
                RealtimeTransformer::catch_panic_pad_function(
                    parent,
                    || false,
                    |transcriber| transcriber.src_query(pad, query),
                )
            })
            .name(format!("realtime_src_{}", state.pad_serial).as_str())
            .flags(gst::PadFlags::FIXED_CAPS)
            .build();

        state.srcpads.insert(pad.clone());

        gst::info!(CAT, "New pad requested, {}", state.srcpads.len());

        state.pad_serial += 1;
        drop(state);

        self.obj().add_pad(&pad).unwrap();

        self.obj().child_added(&pad, &pad.name());

        Some(pad.upcast())
    }

    fn release_pad(&self, pad: &gst::Pad) {
        pad.set_active(false).unwrap();
        self.obj().remove_pad(pad).unwrap();

        self.obj().child_removed(pad, &pad.name());
    }

    fn change_state(
        &self,
        transition: gst::StateChange,
    ) -> Result<gst::StateChangeSuccess, gst::StateChangeError> {
        gst::info!(CAT, imp = self, "Changing state {:?}", transition);

        if transition == gst::StateChange::PausedToReady {
            self.disconnect().map_err(|err| {
                self.post_error_message(err);
                gst::StateChangeError
            })?;
        }

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

    fn provide_clock(&self) -> Option<gst::Clock> {
        Some(gst::SystemClock::obtain())
    }
}

#[derive(Debug)]
struct RealtimeSrcPadState {
    sender: Option<mpsc::Sender<Message>>,
    buffers: VecDeque<gst::Buffer>,
    send_eos: bool,
    out_segment: gst::FormattedSegment<gst::ClockTime>,
    seqnum: gst::Seqnum,
    last_buffer_rtime: Option<gst::ClockTime>,
    current_item_id: Option<String>,
    executor: Executor,
}

impl Default for RealtimeSrcPadState {
    fn default() -> Self {
        Self {
            sender: None,
            buffers: VecDeque::new(),
            send_eos: false,
            out_segment: gst::FormattedSegment::new(),
            seqnum: gst::Seqnum::next(),
            last_buffer_rtime: None,
            current_item_id: None,
            executor: Executor::default(),
        }
    }
}

#[derive(Debug, Default)]
pub struct RealtimeSrcPad {
    state: Mutex<RealtimeSrcPadState>,
}

impl RealtimeSrcPadState {
    fn push_buffer(&mut self, buf: gst::Buffer) {
        self.last_buffer_rtime = match (buf.pts(), buf.duration()) {
            (Some(a), Some(b)) => Some(a + b),
            _ => None,
        };
        self.buffers.push_back(buf);
    }

    fn reset_buffers(&mut self) {
        gst::debug!(
            CAT,
            "Reset output audio buffers: {} chunks",
            self.buffers.len()
        );
        self.buffers.clear();
        self.current_item_id = None;
        self.last_buffer_rtime = None;
    }
}

#[glib::object_subclass]
impl ObjectSubclass for RealtimeSrcPad {
    const NAME: &'static str = "GstOpenaiRealtimeSrcPad";
    type Type = super::RealtimeSrcPad;
    type ParentType = gst::Pad;

    fn new() -> Self {
        Default::default()
    }
}

impl ObjectImpl for RealtimeSrcPad {}

impl GstObjectImpl for RealtimeSrcPad {}

impl PadImpl for RealtimeSrcPad {}
