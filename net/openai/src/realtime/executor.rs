use serde::{Deserialize, Serialize};
use std::{io::Write, net::TcpStream};

#[derive(Debug)]
pub struct Executor {
    stream: Option<TcpStream>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FunctionCall {
    pub name: String,
    pub arguments: serde_json::Map<String, serde_json::Value>,
}

impl FunctionCall {
    fn remove_surrounding_chars(
        input_str: &str,
        start_char: char,
        end_char: char,
    ) -> Option<String> {
        if let (Some(start_pos), Some(end_pos)) =
            (input_str.find(start_char), input_str.rfind(end_char))
        {
            let result = input_str[start_pos..end_pos + 1].to_string();
            Some(result)
        } else {
            None
        }
    }

    fn try_parse_arguments(input_str: &str) -> Option<serde_json::Map<String, serde_json::Value>> {
        Self::remove_surrounding_chars(input_str, '{', '}')
            .and_then(|x| serde_json::from_str(&x).ok())
    }
    pub fn from(name: &str, arguments: &str) -> Option<Self> {
        Self::try_parse_arguments(arguments).map(|x| Self {
            name: name.to_string(),
            arguments: x,
        })
    }
}

impl Executor {
    pub fn default() -> Self {
        Self { stream: None }
    }

    pub fn ensure_connection(&mut self, addr: &Option<String>) -> Result<(), gst::ErrorMessage> {
        if self.stream.is_some() {
            return Ok(());
        }

        if let Some(addr) = addr {
            let stream = TcpStream::connect(addr).map_err(|x| {
                gst::error_msg!(
                    gst::CoreError::Failed,
                    ["Failed to connect {}, error: {}", addr, x]
                )
            })?;
            self.stream = Some(stream);

            Ok(())
        } else {
            Err(gst::error_msg!(gst::CoreError::Failed, ["Addr is not set"]))
        }
    }

    pub fn execute(&self, fc: &FunctionCall) -> Result<(), gst::ErrorMessage> {
        if self.stream.is_none() {
            return Err(gst::error_msg!(
                gst::CoreError::Failed,
                ["Not connected to executor"]
            ));
        }

        let mut content = serde_json::to_vec(fc).map_err(|e| {
            gst::error_msg!(
                gst::CoreError::Failed,
                ["Failed to serialize FunctionCall: {}", e]
            )
        })?;
        let mut msg = (content.len() as u32).to_be_bytes().to_vec();
        msg.append(&mut content);
        
        let res = match self.stream.as_ref().unwrap().try_clone() {
            Ok(mut stream) => {
                match stream.write_all(&msg) {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        Err(gst::error_msg!(
                            gst::CoreError::Failed, 
                            ["Failed to send contents {}", e]
                        ))
                    }
                }
            },
            Err(e) => {
                Err(gst::error_msg!(
                    gst::CoreError::Failed,
                    ["Failed to clone stream {}", e]
                ))
            }
        };

        res
    }

    pub fn reset_connection(&mut self) {
        self.stream = None;
    }

    pub fn is_connected(&self) -> bool {
        self.stream.is_some()
    }
}

#[test]
fn test_incomplete_arguments() {
    let s = "{\"motion_tag\": \"turn_left\"}\n 好的，我现在向左转。";
    let obj = FunctionCall::try_parse_arguments(s);
    assert!(obj.is_some());
}
