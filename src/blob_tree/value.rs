// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::coding::{Decode, DecodeError, Encode, EncodeError};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};
use value_log::{Slice, UserValue, ValueHandle};

/// A value which may or may not be inlined into an index tree
///
/// If not inlined, the value is present in the value log, so it needs
/// to be fetched using the given value handle.
#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub enum MaybeInlineValue {
    /// Inlined value (classic LSM-tree)
    Inline(UserValue),

    /// The value is a handle (pointer) into the value log
    Indirect { vhandle: ValueHandle, size: u32 },
}

impl Encode for ValueHandle {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        writer.write_u64::<BigEndian>(self.offset)?;
        writer.write_u64::<BigEndian>(self.segment_id)?;
        Ok(())
    }
}

impl Decode for ValueHandle {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let offset = reader.read_u64::<BigEndian>()?;
        let segment_id = reader.read_u64::<BigEndian>()?;
        Ok(Self { segment_id, offset })
    }
}

const TAG_INLINE: u8 = 0;
const TAG_INDIRECT: u8 = 1;

impl Encode for MaybeInlineValue {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        match self {
            Self::Inline(bytes) => {
                writer.write_u8(TAG_INLINE)?;

                // NOTE: Values can be up to 2^32 bytes
                #[allow(clippy::cast_possible_truncation)]
                writer.write_u32::<BigEndian>(bytes.len() as u32)?;

                writer.write_all(bytes)?;
            }
            Self::Indirect { vhandle, size } => {
                writer.write_u8(TAG_INDIRECT)?;
                vhandle.encode_into(writer)?;
                writer.write_u32::<BigEndian>(*size)?;
            }
        }
        Ok(())
    }
}

impl Decode for MaybeInlineValue {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let tag = reader.read_u8()?;

        match tag {
            TAG_INLINE => {
                let len = reader.read_u32::<BigEndian>()? as usize;
                let mut bytes = vec![0; len];
                reader.read_exact(&mut bytes)?;
                Ok(Self::Inline(Slice::from(bytes)))
            }
            TAG_INDIRECT => {
                let vhandle = ValueHandle::decode_from(reader)?;
                let size = reader.read_u32::<BigEndian>()?;
                Ok(Self::Indirect { vhandle, size })
            }
            x => Err(DecodeError::InvalidTag(("MaybeInlineValue", x))),
        }
    }
}
