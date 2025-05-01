use std::sync::Mutex;
use std::time::Duration;

use gst::glib;
use gst::prelude::*;
use gst::subclass::prelude::*;

use tokio::sync::mpsc::{self, Sender};
use tokio_tungstenite::{
    connect_async, 
    tungstenite::protocol::Message as WsMessage, 
};
// 修正 futures 导入
use futures::future::AbortHandle as FuturesAbortHandle;
use futures::SinkExt;
use futures::StreamExt;
use url::Url;
use once_cell::sync::Lazy;

// 创建一个全局的 Tokio 运行时
lazy_static::lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
}

// 日志分类
static CAT: Lazy<gst::DebugCategory> = Lazy::new(|| {
    gst::DebugCategory::new(
        "websocketvideosink",
        gst::DebugColorFlags::empty(),
        Some("Ticos WebSocket Video Element"),
    )
});

// 插件设置
#[derive(Debug, Clone)]
struct Settings {
    url: Option<String>,
    api_key: Option<String>,
    connection_timeout_ms: u32,
    // 重连间隔（毫秒）
    reconnect_delay_ms: u32,
    // 最大重试次数，0表示不重连
    reconnect_attempts: u32,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            url: None,
            api_key: None,
            connection_timeout_ms: 5000,
            reconnect_delay_ms: 5000,
            reconnect_attempts: 3,
        }
    }
}

// 状态管理
struct State {
    connected: bool,
    // 用于中止WebSocket接收任务的句柄
    recv_abort_handle: Option<FuturesAbortHandle>,
    // 用于中止WebSocket发送任务的句柄
    send_abort_handle: Option<FuturesAbortHandle>,
    // 用于向WebSocket发送任务发送数据的发送器
    sender: Option<Sender<Vec<u8>>>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            connected: false,
            recv_abort_handle: None,
            send_abort_handle: None,
            sender: None,
        }
    }
}

// 消息格式定义
struct VideoMessage {
    data: Vec<u8>,
}

impl VideoMessage {
    fn new(jpeg_data: &[u8]) -> Self {
        let mut data = Vec::with_capacity(jpeg_data.len() + 12);
        // 同步头 (0x54)
        data.push(0x54);
        // 消息类型 (0x20 表示视频帧)
        data.push(0x20);
        
        // 消息ID (4字节) - 使用简单递增ID
        static mut MESSAGE_ID: u32 = 0;
        let id = unsafe {
            MESSAGE_ID += 1;
            MESSAGE_ID
        };
        data.extend_from_slice(&id.to_be_bytes());
        
        // 消息长度 (4字节) - JPEG数据的长度
        let length = jpeg_data.len() as u32;
        data.extend_from_slice(&length.to_be_bytes());
        
        // JPEG数据
        data.extend_from_slice(jpeg_data);
        
        // 校验码 (0x00)
        data.push(0x00);
        
        Self { data }
    }
    
    fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

// WebsocketVideoSink元素的实现
pub struct WebsocketVideoSink {
    // 输入pad
    sinkpad: gst::Pad,
    // 元素设置
    settings: Mutex<Settings>,
    // 元素状态
    state: Mutex<State>,
}

// GStreamer元素子类实现
#[glib::object_subclass]
impl ObjectSubclass for WebsocketVideoSink {
    const NAME: &'static str = "GstTicosWebsocketVideoSink";
    type Type = super::WebsocketVideoSink;
    type ParentType = gst::Element;
    
    fn with_class(klass: &Self::Class) -> Self {
        // 初始化日志分类（只在第一次使用时初始化）
        // CAT已通过once_cell::Lazy自动处理初始化
        
        // 创建输入pad
        let templ = klass.pad_template("sink").unwrap();
        let sinkpad = gst::Pad::builder_from_template(&templ)
            .chain_function(|pad, parent, buffer| {
                WebsocketVideoSink::catch_panic_pad_function(
                    parent,
                    || Err(gst::FlowError::Error),
                    |sink| sink.sink_chain(pad, buffer),
                )
            })
            .event_function(|pad, parent, event| {
                WebsocketVideoSink::catch_panic_pad_function(
                    parent,
                    || false,
                    |sink| sink.sink_event(pad, event),
                )
            })
            .build();
            
        Self {
            sinkpad,
            settings: Mutex::new(Settings::default()),
            state: Mutex::new(State::default()),
        }
    }
}

// 实现接收缓冲区的链函数
impl WebsocketVideoSink {
    fn sink_chain(
        &self,
        _pad: &gst::Pad,
        buffer: gst::Buffer,
    ) -> Result<gst::FlowSuccess, gst::FlowError> {
        // 确保WebSocket连接已建立
        if let Err(err) = self.ensure_connection() {
            gst::error!(*CAT, imp = self, "Failed to ensure connection: {}", err);
            return Err(gst::FlowError::Error);
        }
        
        // 读取缓冲区数据
        let map = buffer.map_readable().map_err(|_| {
            gst::error!(*CAT, imp = self, "Failed to map buffer readable");
            gst::FlowError::Error
        })?;
        
        let data = map.as_slice();
        let message = VideoMessage::new(data);
        
        // 通过mpsc通道发送数据到WebSocket发送任务
        let mut state = self.state.lock().unwrap();
        if let Some(sender) = &state.sender {
            // 克隆数据以便在tokio任务中使用
            let message_data = message.as_bytes().to_vec();
            
            // 尝试发送到通道
            if let Err(err) = sender.try_send(message_data.clone()) {
                match err {
                    mpsc::error::TrySendError::Full(_) => {
                        gst::warning!(*CAT, imp = self, "Channel full, dropping frame");
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        gst::warning!(*CAT, imp = self, "Channel closed, attempting to reconnect");
                        // 释放锁，以便重连函数可以获取锁
                        drop(state);
                        
                        // 尝试重连
                        if let Err(err) = self.try_reconnect() {
                            gst::error!(*CAT, imp = self, "Failed to reconnect: {}", err);
                            return Err(gst::FlowError::Error);
                        }
                        
                        // 重新获取状态并重试发送
                        state = self.state.lock().unwrap();
                        if let Some(new_sender) = &state.sender {
                            if let Err(err) = new_sender.try_send(message_data) {
                                gst::error!(*CAT, imp = self, "Failed to send after reconnect: {}", err);
                                return Err(gst::FlowError::Error);
                            }
                        } else {
                            gst::error!(*CAT, imp = self, "No sender available after reconnect");
                            return Err(gst::FlowError::Error);
                        }
                    }
                }
            }
        } else {
            gst::error!(*CAT, imp = self, "No sender available");
            return Err(gst::FlowError::Error);
        }
        
        Ok(gst::FlowSuccess::Ok)
    }
    
    // 处理事件
    fn sink_event(&self, _pad: &gst::Pad, event: gst::Event) -> bool {
        use gst::EventView;
        
        match event.view() {
            EventView::Eos(_) => {
                // 收到EOS事件时断开连接
                if let Err(err) = self.disconnect() {
                    gst::error!(*CAT, imp = self, "Failed to disconnect: {}", err);
                    false
                } else {
                    true
                }
            }
            // 处理其他类型的事件
            _ => true,
        }
    }
    
    // 确保WebSocket连接已建立
    fn ensure_connection(&self) -> Result<(), gst::ErrorMessage> {
        let mut state = self.state.lock().unwrap();
        let settings = self.settings.lock().unwrap();
        
        if state.connected {
            return Ok(());
        }
        
        gst::info!(*CAT, imp = self, "Connecting...");
        
        let url = match &settings.url {
            Some(url) => url.to_string(),
            None => {
                return Err(gst::error_msg!(
                    gst::CoreError::Failed,
                    ["No WebSocket URL provided"]
                ));
            }
        };
        
        let uri = Url::parse(&url).map_err(|e| {
            gst::error_msg!(
                gst::CoreError::Failed,
                ["Failed to parse provided url: {}", e]
            )
        })?;
        
        // 构建WebSocket请求
        let mut request_builder = http::Request::builder()
            .method("GET")
            .uri(&url)
            .header("Host", uri.host_str().unwrap_or(""))
            .header("Upgrade", "websocket")
            .header("Connection", "Upgrade")
            .header(
                "Sec-WebSocket-Key",
                tokio_tungstenite::tungstenite::handshake::client::generate_key(),
            )
            .header("Sec-WebSocket-Version", "13");
        
        // 添加API密钥（如果有）
        if let Some(api_key) = &settings.api_key {
            request_builder = request_builder.header("Authorization", format!("Bearer {}", api_key));
        }
        
        let request = request_builder.body(()).map_err(|e| {
            gst::error_msg!(
                gst::CoreError::Failed,
                ["Failed to build request: {}", e]
            )
        })?;
        
        // 创建WebSocket连接
        let connect_future = connect_async(request);
        let timeout_duration = Duration::from_millis(settings.connection_timeout_ms as u64);
        
        let (ws_stream, _) = RUNTIME
            .block_on(async move {
                tokio::time::timeout(timeout_duration, connect_future).await
            })
            .map_err(|e| {
                gst::error_msg!(
                    gst::CoreError::Failed,
                    ["Connection timeout: {}", e]
                )
            })?
            .map_err(|e| {
                gst::error_msg!(
                    gst::CoreError::Failed,
                    ["WebSocket connection error: {}", e]
                )
            })?;
        
        // 创建MPSC通道用于发送数据
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(32);
        
        // 分离WebSocket流的发送和接收部分
        let (mut ws_sink, mut ws_stream) = ws_stream.split();
        
        // 创建发送任务
        let send_future = async move {
            while let Some(data) = rx.recv().await {
                if let Err(err) = ws_sink.send(WsMessage::Binary(data)).await {
                    gst::error!(
                        *CAT,
                        "WebSocket send error: {}",
                        err
                    );
                    break;
                }
            }
        };
        
        let (send_future, send_abort_handle) = futures::future::abortable(send_future);
        
        // 创建接收任务（仅用于保持连接并处理任何传入消息）
        let recv_future = async move {
            while let Some(message_result) = ws_stream.next().await {
                match message_result {
                    Ok(message) => {
                        match message {
                            WsMessage::Text(text) => {
                                gst::trace!(*CAT, "Received text message: {}", text);
                            }
                            WsMessage::Binary(data) => {
                                gst::trace!(*CAT, "Received binary message: {} bytes", data.len());
                            }
                            WsMessage::Ping(_) => {
                                // tungstenite自动响应Ping
                                gst::trace!(*CAT, "Received ping");
                            }
                            WsMessage::Pong(_) => {
                                gst::trace!(*CAT, "Received pong");
                            }
                            WsMessage::Close(_) => {
                                gst::info!(*CAT, "WebSocket closed by server");
                                break;
                            }
                            WsMessage::Frame(_) => {
                                // 框架级消息，通常不直接处理
                            }
                        }
                    }
                    Err(err) => {
                        gst::error!(*CAT, "WebSocket receive error: {}", err);
                        break;
                    }
                }
            }
            
            gst::info!(*CAT, "WebSocket receiver task ended");
        };
        
        let (recv_future, recv_abort_handle) = futures::future::abortable(recv_future);
        
        // 启动任务
        RUNTIME.spawn(send_future);
        RUNTIME.spawn(recv_future);
        
        // 更新状态
        state.connected = true;
        state.send_abort_handle = Some(send_abort_handle);
        state.recv_abort_handle = Some(recv_abort_handle);
        state.sender = Some(tx);
        
        gst::info!(*CAT, imp = self, "Connected");
        
        Ok(())
    }
    
    // 断开WebSocket连接
    fn disconnect(&self) -> Result<(), gst::ErrorMessage> {
        let mut state = self.state.lock().unwrap();
        
        gst::info!(*CAT, imp = self, "Disconnecting");
        
        // 中止接收任务
        if let Some(abort_handle) = state.recv_abort_handle.take() {
            abort_handle.abort();
        }
        
        // 中止发送任务
        if let Some(abort_handle) = state.send_abort_handle.take() {
            abort_handle.abort();
        }
        
        // 丢弃发送器
        state.sender = None;
        state.connected = false;
        
        gst::info!(*CAT, imp = self, "Disconnected");
        
        Ok(())
    }

    // 添加重连函数
    fn try_reconnect(&self) -> Result<(), gst::ErrorMessage> {
        let settings = self.settings.lock().unwrap();
        let max_attempts = settings.reconnect_attempts;
        let delay = Duration::from_millis(settings.reconnect_delay_ms as u64);
        let reconnect_delay_ms = settings.reconnect_delay_ms;
        drop(settings);

        // 先断开现有连接
        if let Err(err) = self.disconnect() {
            gst::warning!(*CAT, imp = self, "Error during disconnect for reconnect: {}", err);
        }

        let mut attempt = 0;
        while attempt < max_attempts {
            attempt += 1;
            gst::info!(*CAT, imp = self, "Reconnection attempt {}/{}", attempt, max_attempts);

            match self.ensure_connection() {
                Ok(_) => {
                    gst::info!(*CAT, imp = self, "Successfully reconnected");
                    return Ok(());
                }
                Err(err) => {
                    if attempt < max_attempts {
                        gst::warning!(*CAT, imp = self, 
                            "Reconnection attempt {} failed: {}. Retrying in {} ms...", 
                            attempt, err, reconnect_delay_ms
                        );
                        // 使用tokio运行时等待指定时间
                        RUNTIME.block_on(async move {
                            tokio::time::sleep(delay).await;
                        });
                    } else {
                        gst::error!(*CAT, imp = self, 
                            "Failed to reconnect after {} attempts", max_attempts
                        );
                        return Err(err);
                    }
                }
            }
        }

        Err(gst::error_msg!(
            gst::CoreError::Failed,
            ["Failed to reconnect after all attempts"]
        ))
    }
}

// 实现 ObjectImpl 特质
impl ObjectImpl for WebsocketVideoSink {
    fn properties() -> &'static [glib::ParamSpec] {
        use glib::ParamFlags;
        use once_cell::sync::Lazy;
        
        static PROPERTIES: Lazy<Vec<glib::ParamSpec>> = Lazy::new(|| {
            vec![
                glib::ParamSpecString::builder("url")
                    .nick("URL")
                    .blurb("WebSocket server URL")
                    .flags(ParamFlags::READWRITE | ParamFlags::CONSTRUCT)
                    .build(),
                glib::ParamSpecString::builder("api-key")
                    .nick("API Key")
                    .blurb("API key for server authentication")
                    .flags(ParamFlags::READWRITE | ParamFlags::CONSTRUCT)
                    .build(),
                glib::ParamSpecUInt::builder("connection-timeout-ms")
                    .nick("Connection Timeout")
                    .blurb("Connection timeout in milliseconds")
                    .flags(ParamFlags::READWRITE | ParamFlags::CONSTRUCT)
                    .default_value(5000)
                    .build(),
                glib::ParamSpecUInt::builder("reconnect-delay-ms")
                    .nick("Reconnect Delay")
                    .blurb("Time between connection attempts in milliseconds")
                    .flags(ParamFlags::READWRITE | ParamFlags::CONSTRUCT)
                    .default_value(5000)
                    .build(),
                glib::ParamSpecUInt::builder("reconnect-attempts")
                    .nick("Reconnect Attempts")
                    .blurb("Maximum number of reconnection attempts")
                    .flags(ParamFlags::READWRITE | ParamFlags::CONSTRUCT)
                    .default_value(3)
                    .build(),
            ]
        });
        
        PROPERTIES.as_ref()
    }
    
    fn set_property(&self, _id: usize, value: &glib::Value, pspec: &glib::ParamSpec) {
        match pspec.name() {
            "url" => {
                let mut settings = self.settings.lock().unwrap();
                settings.url = value.get().expect("URL value is not a string");
            }
            "api-key" => {
                let mut settings = self.settings.lock().unwrap();
                settings.api_key = value.get().expect("API key value is not a string");
            }
            "connection-timeout-ms" => {
                let mut settings = self.settings.lock().unwrap();
                settings.connection_timeout_ms = value.get().expect("Connection timeout value is not a uint");
            }
            "reconnect-delay-ms" => {
                let mut settings = self.settings.lock().unwrap();
                settings.reconnect_delay_ms = value.get().expect("Reconnect delay value is not a uint");
            }
            "reconnect-attempts" => {
                let mut settings = self.settings.lock().unwrap();
                settings.reconnect_attempts = value.get().expect("Reconnect attempts value is not a uint");
            }
            _ => unreachable!(),
        }
    }
    
    fn property(&self, _id: usize, pspec: &glib::ParamSpec) -> glib::Value {
        match pspec.name() {
            "url" => {
                let settings = self.settings.lock().unwrap();
                settings.url.to_value()
            }
            "api-key" => {
                let settings = self.settings.lock().unwrap();
                settings.api_key.to_value()
            }
            "connection-timeout-ms" => {
                let settings = self.settings.lock().unwrap();
                settings.connection_timeout_ms.to_value()
            }
            "reconnect-delay-ms" => {
                let settings = self.settings.lock().unwrap();
                settings.reconnect_delay_ms.to_value()
            }
            "reconnect-attempts" => {
                let settings = self.settings.lock().unwrap();
                settings.reconnect_attempts.to_value()
            }
            _ => unreachable!(),
        }
    }
    
    fn constructed(&self) {
        self.parent_constructed();
        
        let obj = self.obj();
        
        // 将sink pad添加到元素
        obj.add_pad(&self.sinkpad).unwrap();
    }
}

// 实现 GstObjectImpl 特质
impl GstObjectImpl for WebsocketVideoSink {}

// 实现 ElementImpl 特质
impl ElementImpl for WebsocketVideoSink {
    fn metadata() -> Option<&'static gst::subclass::ElementMetadata> {
        static ELEMENT_METADATA: Lazy<gst::subclass::ElementMetadata> = Lazy::new(|| {
            gst::subclass::ElementMetadata::new(
                "WebSocket Video Sink",
                "Sink/Video/Network",
                "Sends video frames over WebSocket",
                "Ticos Team <info@ticos.ai>",
            )
        });
        
        Some(&*ELEMENT_METADATA)
    }
    
    fn pad_templates() -> &'static [gst::PadTemplate] {
        static PAD_TEMPLATES: Lazy<Vec<gst::PadTemplate>> = Lazy::new(|| {
            let caps = gst::Caps::builder("image/jpeg").build();
            let sink_pad_template = gst::PadTemplate::new(
                "sink",
                gst::PadDirection::Sink,
                gst::PadPresence::Always,
                &caps,
            )
            .unwrap();
            
            vec![sink_pad_template]
        });
        
        PAD_TEMPLATES.as_ref()
    }
    
    fn change_state(
        &self,
        transition: gst::StateChange,
    ) -> Result<gst::StateChangeSuccess, gst::StateChangeError> {
        match transition {
            gst::StateChange::NullToReady => {
                // 元素进入Ready状态时不需要做任何特殊处理
            }
            gst::StateChange::ReadyToPaused => {
                // 元素进入Paused状态时，尝试建立WebSocket连接
                // 但我们会推迟到第一个缓冲区到达时
            }
            gst::StateChange::PausedToPlaying => {
                // 元素进入Playing状态，确保连接已建立
                if let Err(err) = self.ensure_connection() {
                    gst::error!(*CAT, imp = self, "Failed to connect: {}", err);
                    return Err(gst::StateChangeError);
                }
            }
            gst::StateChange::PlayingToPaused => {
                // 元素回到Paused状态，保持连接
            }
            gst::StateChange::PausedToReady => {
                // 元素回到Ready状态，断开连接
                if let Err(err) = self.disconnect() {
                    gst::error!(*CAT, imp = self, "Failed to disconnect: {}", err);
                }
            }
            gst::StateChange::ReadyToNull => {
                // 元素回到Null状态，确保已断开连接
                if let Err(err) = self.disconnect() {
                    gst::error!(*CAT, imp = self, "Failed to disconnect: {}", err);
                }
            }
            _ => (),
        }
        
        // 调用父类的状态变更实现
        self.parent_change_state(transition)
    }
} 