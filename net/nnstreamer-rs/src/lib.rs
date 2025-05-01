use libc::{c_char, c_int, c_void, size_t};

pub const NNS_TENSOR_RANK_LIMIT: usize = 8;

pub const TENSOR_META_MAGIC: u32 = 0xfeedcced;

pub const fn make_tensor_meta_version(major: u32, minor: u32) -> u32 {
    (major << 12) | minor | 0xDE000000
}

pub const TENSOR_META_VERSION: u32 = make_tensor_meta_version(1, 0);

#[repr(C)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TensorType {
    Int32,
    UInt32,
    Int16,
    UInt16,
    Int8,
    UInt8,
    Float64,
    Float32,
    Int64,
    UInt64,
    Float16,
    End,
}

impl TensorType {
    pub fn typename(&self) -> Option<&'static str> {
        match self {
            TensorType::Int32 => Some("int32"),
            TensorType::UInt32 => Some("uint32"),
            TensorType::Int16 => Some("int16"),
            TensorType::UInt16 => Some("uint16"),
            TensorType::Int8 => Some("int8"),
            TensorType::UInt8 => Some("uint8"),
            TensorType::Float64 => Some("float64"),
            TensorType::Float32 => Some("float32"),
            TensorType::Int64 => Some("int64"),
            TensorType::UInt64 => Some("uint64"),
            TensorType::Float16 => Some("float16"),
            TensorType::End => None,
        }
    }

    pub fn element_size(&self) -> usize {
        match self {
            TensorType::Int32 | TensorType::UInt32 | TensorType::Float32 => 4,
            TensorType::Int16 | TensorType::UInt16 | TensorType::Float16 => 2,
            TensorType::Int8 | TensorType::UInt8 => 1,
            TensorType::Float64 | TensorType::Int64 | TensorType::UInt64 => 8,
            TensorType::End => 0,
        }
    }
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MediaType {
    Video,
    Audio,
    Text,
    Octet,
    Tensor,
    Any = 0x1000,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TensorFormat {
    Static,
    Flexible,
    Sparse,
    End,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TensorLayout {
    Any,
    Nhwc,
    Nchw,
    None,
}

pub type TensorDim = [u32; NNS_TENSOR_RANK_LIMIT];

pub fn tensor_get_element_count(dim: &[u32; NNS_TENSOR_RANK_LIMIT]) -> u64 {
    let mut count: u64 = 1;

    for &d in dim.iter() {
        if d == 0 {
            break;
        }
        count *= d as u64;
    }

    if count > 1 {
        count
    } else {
        0
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct TensorMemory {
    pub data: *mut c_void, // pointer to mapped gstreamer memory
    pub size: size_t,
}

#[repr(C)]
#[derive(Debug)]
pub struct TensorInfo {
    pub name: *const c_char,
    pub tensor_type: TensorType,
    pub tensor_dim: TensorDim, // NNstreamer framework supports up to 8th rank
}

#[repr(C)]
#[derive(Debug)]
pub struct TensorsInfo {
    pub num_tensors: c_int,
    pub info: [TensorInfo; NNS_TENSOR_RANK_LIMIT],
}

#[repr(C)]
#[derive(Debug)]
pub struct TensorsConfig {
    pub info: TensorsInfo,
    pub rate_n: c_int, // framerate is in fraction, which is numerator/denominator
    pub rate_d: c_int, // framerate is in fraction, which is numerator/denominator
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct TensorMetaInfo {
    pub magic: u32,
    pub version: u32,
    pub tensor_type: TensorType,
    pub dimension: TensorDim,
    pub format: TensorFormat,
    pub media_type: MediaType,
    pub nnz: u32,
}

impl Default for TensorMetaInfo {
    fn default() -> Self {
        TensorMetaInfo {
            magic: TENSOR_META_MAGIC,
            version: TENSOR_META_VERSION,
            tensor_type: TensorType::End,
            dimension: [0; NNS_TENSOR_RANK_LIMIT],
            format: TensorFormat::Static,
            media_type: MediaType::Tensor,
            nnz: 0,
        }
    }
} // TensorMetaInfo

impl TensorMetaInfo {
    pub fn is_version_valid(&self) -> bool {
        (self.version & 0xDE000000) == 0xDE000000
    }

    pub fn is_valid(&self) -> bool {
        self.magic == TENSOR_META_MAGIC && self.is_version_valid()
    }

    pub fn get_version(&self) -> Result<(u32, u32), TensorMetaError> {
        if !self.is_valid() {
            return Err(TensorMetaError::InvalidMeta);
        }

        let major = (self.version & 0x00FFF000) >> 12;
        let minor = self.version & 0x00000FFF;

        Ok((major, minor))
    }

    pub fn get_header_size(&self) -> usize {
        if !self.is_valid() {
            return 0;
        }

        // Assuming GST_TENSOR_META_IS_V1 checks if the version is 1.0
        if self.version == TENSOR_META_VERSION {
            return 128;
        }

        0
    }
}

#[derive(Debug)]
pub enum TensorMetaError {
    InvalidMeta,
}
