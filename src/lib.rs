pub use arrow;

#[cfg(feature = "fs")]
pub mod fs;

use std::io;

use std::collections::BTreeSet;

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

pub fn ints2uniq_bt<I, T>(ints: I) -> BTreeSet<T::Native>
where
    T: ArrowPrimitiveType,
    T::Native: Ord,
    I: Iterator<Item = T::Native>,
{
    BTreeSet::from_iter(ints)
}

pub fn uniq2arrow_bt<T>(uniq: BTreeSet<T::Native>) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: Ord,
{
    PrimitiveArray::from_iter_values(uniq)
}

pub fn ints2uniq2arrow_bt<I, T>(ints: I) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: Ord,
    I: Iterator<Item = T::Native>,
{
    let uniq: BTreeSet<T::Native> = ints2uniq_bt::<I, T>(ints);
    uniq2arrow_bt(uniq)
}

macro_rules! create_ints2uniq2arrow_bt {
    ($fname: ident, $ity: ty) => {
        /// Creates an array of unique integers from the integers.
        pub fn $fname<I>(ints: I) -> PrimitiveArray<$ity>
        where
            I: Iterator<Item = <$ity as ArrowPrimitiveType>::Native>,
        {
            ints2uniq2arrow_bt(ints)
        }
    };
}

create_ints2uniq2arrow_bt!(ints2uniq2arrow_bt8, Int8Type);
create_ints2uniq2arrow_bt!(ints2uniq2arrow_bt16, Int16Type);
create_ints2uniq2arrow_bt!(ints2uniq2arrow_bt32, Int32Type);
create_ints2uniq2arrow_bt!(ints2uniq2arrow_bt64, Int64Type);

create_ints2uniq2arrow_bt!(uints2uniq2arrow_bt8, UInt8Type);
create_ints2uniq2arrow_bt!(uints2uniq2arrow_bt16, UInt16Type);
create_ints2uniq2arrow_bt!(uints2uniq2arrow_bt32, UInt32Type);
create_ints2uniq2arrow_bt!(uints2uniq2arrow_bt64, UInt64Type);

pub fn rints2uniq_bt<I, T>(ints: I) -> Result<BTreeSet<T::Native>, io::Error>
where
    T: ArrowPrimitiveType,
    T::Native: Ord,
    I: Iterator<Item = Result<T::Native, io::Error>>,
{
    ints.collect()
}

pub fn rints2uniq2arrow_bt<I, T>(ints: I) -> Result<PrimitiveArray<T>, io::Error>
where
    T: ArrowPrimitiveType,
    T::Native: Ord,
    I: Iterator<Item = Result<T::Native, io::Error>>,
{
    let uniq: BTreeSet<T::Native> = rints2uniq_bt::<I, T>(ints)?;
    Ok(uniq2arrow_bt(uniq))
}

macro_rules! create_rints2uniq2arrow_bt {
    ($fname: ident, $ity: ty) => {
        /// Tries to create an array of unique integers from the integers.
        pub fn $fname<I>(ints: I) -> Result<PrimitiveArray<$ity>, io::Error>
        where
            I: Iterator<Item = Result<<$ity as ArrowPrimitiveType>::Native, io::Error>>,
        {
            rints2uniq2arrow_bt(ints)
        }
    };
}

create_rints2uniq2arrow_bt!(rints2uniq2arrow_bt8, Int8Type);
create_rints2uniq2arrow_bt!(rints2uniq2arrow_bt16, Int16Type);
create_rints2uniq2arrow_bt!(rints2uniq2arrow_bt32, Int32Type);
create_rints2uniq2arrow_bt!(rints2uniq2arrow_bt64, Int64Type);

create_rints2uniq2arrow_bt!(ruints2uniq2arrow_bt8, UInt8Type);
create_rints2uniq2arrow_bt!(ruints2uniq2arrow_bt16, UInt16Type);
create_rints2uniq2arrow_bt!(ruints2uniq2arrow_bt32, UInt32Type);
create_rints2uniq2arrow_bt!(ruints2uniq2arrow_bt64, UInt64Type);
