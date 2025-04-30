use async_tungstenite::tungstenite::Message;
use serde::{Deserialize, Serialize};

use crate::realtime::types::{Item, Session};

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct SessionUpdate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    pub session: Session,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct InputAudioBufferAppend {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    pub audio: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct InputAudioBufferCommit {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct InputAudioBufferClear {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ConversationItemCreate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_item_id: Option<String>,
    pub item: Item,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ConversationItemTruncate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    pub item_id: String,
    pub content_index: u32,
    pub audio_end_ms: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ConversationItemDelete {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    pub item_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ResponseCreate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ResponseCancel {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ClientEvent {
    #[serde(rename = "session.update")]
    SessionUpdate(SessionUpdate),
    #[serde(rename = "input_audio_buffer.append")]
    InputAudioBufferAppend(InputAudioBufferAppend),
    #[serde(rename = "input_audio_buffer.commit")]
    InputAudioBufferCommit(InputAudioBufferCommit),
    #[serde(rename = "input_audio_buffer.clear")]
    InputAudioBufferClear(InputAudioBufferClear),
    #[serde(rename = "conversation.item.create")]
    ConversationItemCreate(ConversationItemCreate),
    #[serde(rename = "conversation.item.truncate")]
    ConversationItemTruncate(ConversationItemTruncate),
    #[serde(rename = "conversation.item.delete")]
    ConversationItemDelete(ConversationItemDelete),
    #[serde(rename = "response.create")]
    ResponseCreate(ResponseCreate),
    #[serde(rename = "response.cancel")]
    ResponseCancel(ResponseCancel),
}

impl From<ClientEvent> for Message {
    fn from(value: ClientEvent) -> Self {
        Message::Text(String::from(&value))
    }
}

impl From<&ClientEvent> for String {
    fn from(value: &ClientEvent) -> Self {
        serde_json::to_string(value).unwrap()
    }
}

impl From<ConversationItemCreate> for Message {
    fn from(value: ConversationItemCreate) -> Self {
        Self::from(ClientEvent::ConversationItemCreate(value))
    }
}

impl From<InputAudioBufferAppend> for Message {
    fn from(value: InputAudioBufferAppend) -> Self {
        Self::from(ClientEvent::InputAudioBufferAppend(value))
    }
}

impl From<InputAudioBufferCommit> for Message {
    fn from(value: InputAudioBufferCommit) -> Self {
        Self::from(ClientEvent::InputAudioBufferCommit(value))
    }
}

impl From<InputAudioBufferClear> for Message {
    fn from(value: InputAudioBufferClear) -> Self {
        Self::from(ClientEvent::InputAudioBufferClear(value))
    }
}

impl From<SessionUpdate> for Message {
    fn from(value: SessionUpdate) -> Self {
        Self::from(ClientEvent::SessionUpdate(value))
    }
}

impl From<ConversationItemTruncate> for Message {
    fn from(value: ConversationItemTruncate) -> Self {
        Self::from(ClientEvent::ConversationItemTruncate(value))
    }
}

impl From<ConversationItemDelete> for Message {
    fn from(value: ConversationItemDelete) -> Self {
        Self::from(ClientEvent::ConversationItemDelete(value))
    }
}

impl From<ResponseCreate> for Message {
    fn from(value: ResponseCreate) -> Self {
        Self::from(ClientEvent::ResponseCreate(value))
    }
}

impl From<ResponseCancel> for Message {
    fn from(value: ResponseCancel) -> Self {
        Self::from(ClientEvent::ResponseCancel(value))
    }
}

#[test]
fn test_session_update() {
    let resps = vec![
        r#"
 {
  "event_id": "evt_uEe5UHQSMYCdKeb9U",
  "type": "session.update",
  "session": {
    "model": {
      "name": "stardust-2.5-max",
      "tools": [
        {
          "id": "1732639265718",
          "code": "def say_with_motion_and_emotion(motion: str, emotion: str) -> str:\n    \"\"\"\n    Generate response with specific motion and emotion.\n    \n    Args:\n        motion: The robot's motion (e.g., wave, nod, shake)\n        emotion: The robot's emotion (e.g., happy, sad, neutral)\n        \n    Returns:\n        str: Response with motion and emotion\n    \"\"\"\n    # Implementation goes here\n    return f\"Responding with motion: {motion}, emotion: {emotion}\"",
          "name": "Expressive Response",
          "type": "function",
          "language": "python",
          "platform": "linux",
          "required": [
            "motion",
            "emotion"
          ],
          "parameters": {
            "type": "object",
            "properties": {
              "motion": {
                "type": "string",
                "description": "The humanoid robot's motion"
              },
              "emotion": {
                "type": "string",
                "description": "The humanoid robot's emotion"
              }
            }
          },
          "description": "Generate response with motion and emotion"
        }
      ],
      "provider": "tiwater",
      "modalities": [
        "text",
        "image"
      ],
      "temperature": 0.7,
      "tool_choice": "auto",
      "instructions": "你是太水科技的有实体具身的人形机器人小太，具有行动、视觉、听觉、说话的能力。对话尽量简短自然，不要书面化和MarkDown的语言，请用中文回答问题。",
      "max_response_output_tokens": 4096
    },
    "speech": {
      "voice": "zh_female_popo_mars_bigtts",
      "output_audio_format": "pcm16"
    },
    "hearing": {
      "turn_detection": {
        "type": "server_vad",
        "threshold": 0.66,
        "prefix_padding_ms": 104,
        "silence_duration_ms": 2000
      },
      "input_audio_format": "pcm16"
    }
  },
  "event": {
    "item": {}
  }
}
    "#,
    ];

    for r in resps {
        let event: SessionUpdate = serde_json::from_str(r).unwrap();
        assert!(event.session.model.is_some());
    }
}
