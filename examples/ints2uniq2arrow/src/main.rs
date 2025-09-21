use std::io;
use std::process::ExitCode;

use rs_ints2uniq2arrow::arrow;

use arrow::array::PrimitiveArray;

use rs_ints2uniq2arrow::fs::filename2arrow_i64le;

fn env2filename() -> Result<String, io::Error> {
    std::env::var("ENV_FILENAME_RAW_INTS_64_LE").map_err(io::Error::other)
}

fn sub() -> Result<(), io::Error> {
    let fname: String = env2filename()?;
    let arr: PrimitiveArray<_> = filename2arrow_i64le(fname)?;
    println!("{arr:#?}");
    Ok(())
}

fn main() -> ExitCode {
    match sub() {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
