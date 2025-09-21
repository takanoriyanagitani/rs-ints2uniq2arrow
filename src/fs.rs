use std::collections::BTreeSet;
use std::fs::File;
use std::io;
use std::path::Path;

use io::Read;

use arrow::array::PrimitiveArray;

use arrow::datatypes::ArrowPrimitiveType;

use arrow::datatypes::Int8Type;
use arrow::datatypes::Int16Type;
use arrow::datatypes::Int32Type;
use arrow::datatypes::Int64Type;

use arrow::datatypes::UInt8Type;
use arrow::datatypes::UInt16Type;
use arrow::datatypes::UInt32Type;
use arrow::datatypes::UInt64Type;

pub fn rdr2ints<R, F, const N: usize, T>(
    mut rdr: R,
    buf2int: F,
) -> impl Iterator<Item = Result<T, io::Error>>
where
    R: Read,
    F: Fn([u8; N]) -> T,
{
    let mut buf: [u8; N] = [0; N];
    std::iter::from_fn(move || {
        let rslt = rdr.read_exact(&mut buf);
        match rslt {
            Ok(_) => Some(Ok(buf2int(buf))),
            Err(e) => match e.kind() {
                io::ErrorKind::UnexpectedEof => None,
                _ => Some(Err(e)),
            },
        }
    })
}

#[cfg(feature = "fs_macro_reader2ints")]
pub mod helper_reader2ints {
    use io::Read;
    use std::io;

    macro_rules! create_rdr2ints {
        ($fname: ident, $ity: ty, $buf2int: expr, $sz: literal) => {
            /// Converts the reader to an iterator of integers.
            pub fn $fname<R>(rdr: R) -> impl Iterator<Item = Result<$ity, io::Error>>
            where
                R: Read,
            {
                super::rdr2ints(rdr, $buf2int)
            }
        };
    }

    create_rdr2ints!(rdr2ints8le, i8, i8::from_le_bytes, 1);
    create_rdr2ints!(rdr2ints16le, i16, i16::from_le_bytes, 2);
    create_rdr2ints!(rdr2ints32le, i32, i32::from_le_bytes, 4);
    create_rdr2ints!(rdr2ints64le, i64, i64::from_le_bytes, 8);

    create_rdr2ints!(rdr2ints8be, i8, i8::from_be_bytes, 1);
    create_rdr2ints!(rdr2ints16be, i16, i16::from_be_bytes, 2);
    create_rdr2ints!(rdr2ints32be, i32, i32::from_be_bytes, 4);
    create_rdr2ints!(rdr2ints64be, i64, i64::from_be_bytes, 8);

    create_rdr2ints!(rdr2uints8le, u8, u8::from_le_bytes, 1);
    create_rdr2ints!(rdr2uints16le, u16, u16::from_le_bytes, 2);
    create_rdr2ints!(rdr2uints32le, u32, u32::from_le_bytes, 4);
    create_rdr2ints!(rdr2uints64le, u64, u64::from_le_bytes, 8);

    create_rdr2ints!(rdr2uints8be, u8, u8::from_be_bytes, 1);
    create_rdr2ints!(rdr2uints16be, u16, u16::from_be_bytes, 2);
    create_rdr2ints!(rdr2uints32be, u32, u32::from_be_bytes, 4);
    create_rdr2ints!(rdr2uints64be, u64, u64::from_be_bytes, 8);
}

pub fn filename2ints<P, F, const N: usize, T>(
    filename: P,
    buf2int: F,
) -> Result<impl Iterator<Item = Result<T, io::Error>>, io::Error>
where
    P: AsRef<Path>,
    F: Fn([u8; N]) -> T,
{
    let f = File::open(filename)?;
    let br = io::BufReader::new(f);
    Ok(rdr2ints(br, buf2int))
}

#[cfg(feature = "fs_macro_file2ints")]
pub mod helper_file2ints {
    use std::io;
    use std::path::Path;

    macro_rules! create_filename2ints {
        ($fname: ident, $ity: ty, $buf2int: expr, $sz: literal) => {
            /// Reads the file and converts to an iterator of integers.
            pub fn $fname<P>(
                filename: P,
            ) -> Result<impl Iterator<Item = Result<$ity, io::Error>>, io::Error>
            where
                P: AsRef<Path>,
            {
                super::filename2ints(filename, $buf2int)
            }
        };
    }

    create_filename2ints!(filename2ints8le, i8, i8::from_le_bytes, 1);
    create_filename2ints!(filename2ints16le, i16, i16::from_le_bytes, 2);
    create_filename2ints!(filename2ints32le, i32, i32::from_le_bytes, 4);
    create_filename2ints!(filename2ints64le, i64, i64::from_le_bytes, 8);
    create_filename2ints!(filename2ints8be, i8, i8::from_be_bytes, 1);
    create_filename2ints!(filename2ints16be, i16, i16::from_be_bytes, 2);
    create_filename2ints!(filename2ints32be, i32, i32::from_be_bytes, 4);
    create_filename2ints!(filename2ints64be, i64, i64::from_be_bytes, 8);

    create_filename2ints!(filename2uints8le, u8, u8::from_le_bytes, 1);
    create_filename2ints!(filename2uints16le, u16, u16::from_le_bytes, 2);
    create_filename2ints!(filename2uints32le, u32, u32::from_le_bytes, 4);
    create_filename2ints!(filename2uints64le, u64, u64::from_le_bytes, 8);
    create_filename2ints!(filename2uints8be, u8, u8::from_be_bytes, 1);
    create_filename2ints!(filename2uints16be, u16, u16::from_be_bytes, 2);
    create_filename2ints!(filename2uints32be, u32, u32::from_be_bytes, 4);
    create_filename2ints!(filename2uints64be, u64, u64::from_be_bytes, 8);
}

pub fn filename2arrow<P, F, const N: usize, T>(
    filename: P,
    buf2int: F,
) -> Result<PrimitiveArray<T>, io::Error>
where
    P: AsRef<Path>,
    T: ArrowPrimitiveType,
    T::Native: Ord,
    F: Fn([u8; N]) -> T::Native,
{
    let ints = filename2ints(filename, buf2int)?;
    let bset: BTreeSet<T::Native> = super::rints2uniq_bt::<_, T>(ints)?;
    Ok(super::uniq2arrow_bt(bset))
}

macro_rules! create_filename2arrow {
    ($fname: ident, $ity: ty, $buf2int: expr, $sz: literal) => {
        /// Reads the file and converts to an array of integers.
        pub fn $fname<P>(filename: P) -> Result<PrimitiveArray<$ity>, io::Error>
        where
            P: AsRef<Path>,
        {
            filename2arrow(filename, $buf2int)
        }
    };
}

create_filename2arrow!(filename2arrow_i8le, Int8Type, i8::from_le_bytes, 1);
create_filename2arrow!(filename2arrow_i16le, Int16Type, i16::from_le_bytes, 2);
create_filename2arrow!(filename2arrow_i32le, Int32Type, i32::from_le_bytes, 4);
create_filename2arrow!(filename2arrow_i64le, Int64Type, i64::from_le_bytes, 8);
create_filename2arrow!(filename2arrow_i8be, Int8Type, i8::from_be_bytes, 1);
create_filename2arrow!(filename2arrow_i16be, Int16Type, i16::from_be_bytes, 2);
create_filename2arrow!(filename2arrow_i32be, Int32Type, i32::from_be_bytes, 4);
create_filename2arrow!(filename2arrow_i64be, Int64Type, i64::from_be_bytes, 8);

create_filename2arrow!(filename2arrow_u8le, UInt8Type, u8::from_le_bytes, 1);
create_filename2arrow!(filename2arrow_u16le, UInt16Type, u16::from_le_bytes, 2);
create_filename2arrow!(filename2arrow_u32le, UInt32Type, u32::from_le_bytes, 4);
create_filename2arrow!(filename2arrow_u64le, UInt64Type, u64::from_le_bytes, 8);
create_filename2arrow!(filename2arrow_u8be, UInt8Type, u8::from_be_bytes, 1);
create_filename2arrow!(filename2arrow_u16be, UInt16Type, u16::from_be_bytes, 2);
create_filename2arrow!(filename2arrow_u32be, UInt32Type, u32::from_be_bytes, 4);
create_filename2arrow!(filename2arrow_u64be, UInt64Type, u64::from_be_bytes, 8);
