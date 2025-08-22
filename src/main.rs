use anyhow::{bail, Context, Ok, Result};
use bytes::Buf;
use std::fs::File;
use std::io::{prelude::*, Cursor};

/// The 100 bytes database header at the begining of database file.
/// Also and only parts of the first table - The schema table.
#[derive(Debug)]
pub struct DatabaseHeader {
    /// Offset=16, size=2
    ///
    /// The database page size in bytes.
    /// Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
    page_size: u16,
}

impl DatabaseHeader {
    pub fn from_cursor(cursor: &mut Cursor<&'_ [u8]>) -> Result<Self> {
        cursor
            .read(&mut [0u8; 16])
            .context("failed to read pre-offset bytes")?;

        let page_size = cursor.get_u16();

        cursor
            .read_exact(&mut [0u8; 100 - 16 - 2])
            .context("failed to read post-offset bytes")?;

        Ok(Self { page_size })
    }
}

/// Type of B-tree page.
#[derive(Debug)]
#[repr(u8)]
enum BTreePageType {
    InteriorIndex = 0x02,
    InteriorTable = 0x05,
    LeafIndex = 0x0a,
    LeafTable = 0x0d,
}

impl TryFrom<u8> for BTreePageType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            0x02 => Ok(Self::InteriorIndex),
            0x05 => Ok(Self::InteriorTable),
            0x0a => Ok(Self::LeafIndex),
            0x0d => Ok(Self::LeafTable),
            v => bail!("invalid B-tree page type {v}"),
        }
    }
}

/// The first 8 or 12 bytes in B-tree page.
///
/// See `B-tree Page Header Format` table on https://www.sqlite.org/fileformat2.html
#[derive(Debug)]
struct BTreePageHeader {
    /// Offset=0, size=1
    page_type: BTreePageType,

    /// Offset=1, size=2
    ///
    /// Gives the start of the first freeblock on the page, or is zero if there are no freeblocks.
    free_block_offset: u16,

    /// Offset=3, size=2
    ///
    /// Cells count on this page.
    cells_count: u16,
}

impl BTreePageHeader {
    pub fn from_cursor(cursor: &mut Cursor<&'_ [u8]>) -> Result<Self> {
        let page_type = BTreePageType::try_from(cursor.get_u8()).context("invalid b-tree type")?;
        let free_block_offset = cursor.get_u16();
        let cells_count = cursor.get_u16();

        let total_size = match page_type {
            BTreePageType::InteriorIndex | BTreePageType::InteriorTable => 12,
            BTreePageType::LeafIndex | BTreePageType::LeafTable => 8,
        };

        cursor
            .read_exact(&mut vec![0u8; total_size - 1 - 2 - 2])
            .context("failed to read post-offset bytes")?;

        Ok(Self {
            page_type,
            free_block_offset,
            cells_count,
        })
    }
}

#[derive(Debug)]
pub struct SchemaTable {
    /// Database header.
    db_header: DatabaseHeader,

    /// Page header.
    page_header: BTreePageHeader,

    /// All cells offset.
    cells_offsets: Vec<u16>,

    /// Table data.
    cells: Vec<Record>,
}

impl SchemaTable {
    pub fn from_cursor(cursor: &mut Cursor<&'_ [u8]>) -> Result<Self> {
        let db_header = DatabaseHeader::from_cursor(cursor).context("failed to parse db header")?;
        let page_header =
            BTreePageHeader::from_cursor(cursor).context("failed to parse page_header")?;

        let mut cells_offsets = vec![];
        let cells_count = page_header.cells_count;
        for _ in 0..cells_count {
            cells_offsets.push(cursor.get_u16());
        }
        cells_offsets.sort();

        println!(">>> cells count: {}", cells_count);

        let mut cells = vec![];
        for pos in cells_offsets.iter() {
            cursor.set_position(*pos as u64);
            // Parse record, the schema of schema table info.
            let record = Record::from_cursor(cursor).context("failed to parse page cell")?;
            if record.headers.len() != 5 {
                bail!(
                    "expected 5 columns in schema table, only got {}",
                    record.headers.len()
                )
            }
            cells.push(record);
        }

        Ok(Self {
            db_header,
            page_header,
            cells_offsets,
            cells,
        })
    }

    pub fn table_names(&self) -> Vec<String> {
        let mut table_names = vec![];
        for cell in self.cells.iter() {
            table_names.push(String::from_utf8(cell.payload[2].clone()).unwrap());
        }
        table_names
    }
}

/// Record in cell, carryig data.
///
/// Record contains header and payload where header is `varint`s and payload is `Vec<u8>`s.
///
/// ```console
/// // Size of the record (varint): 120
/// 78
///
/// // The rowid (safe to ignore)
/// 03
///
/// // Record header
/// 07     // Size of record header (varint): 7
///
/// 17     // Serial type for sqlite_schema.type (varint):     23
///        // Size of sqlite_schema.type =                     (23-13)/2 = 5
///
/// 1b     // Serial type for sqlite_schema.name (varint):     27
///        // Size of sqlite_schema.name =                     (27-13)/2 = 7
///
/// 1b     // Serial type for sqlite_schema.tbl_name (varint): 27
///        // Size of sqlite_schema.tbl_name =                 (27-13)/2 = 7
///
/// 01     // Serial type for sqlite_schema.rootpage (varint): 1
///        // 8-bit twos-complement integer
///
/// 81 47  // Serial type for sqlite_schema.sql (varint):      199
///        // Size of sqlite_schema.sql =                      (199-13)/2 = 93
///
/// // Record body
/// 74 61 62 6c 65        // Value of sqlite_schema.type:     "table"
/// 6f 72 61 6e 67 65 73  // Value of sqlite_schema.name:     "oranges"
/// 6f 72 61 6e 67 65 73  // Value of sqlite_schema.tbl_name: "oranges"  <---
/// ...
/// ```
#[derive(Debug)]
pub struct Record {
    /// Cell length, totally.
    ///
    /// The value represents bytes count of current `Cell`, including `length` field itself.
    length: Varint,

    /// Row id.
    row_id: Varint,

    /// Size of the record header.
    header_bytes_count: Varint,

    /// Headers.
    headers: Vec<Varint>,

    /// Payload.
    payload: Vec<Vec<u8>>,
}

impl Record {
    pub fn from_cursor(cursor: &mut Cursor<&'_ [u8]>) -> Result<Self> {
        if !cursor.has_remaining() {
            panic!("cursor ended");
        }

        println!(
            ">>> pos={}, build record, bytes remaining: {}",
            cursor.position(),
            cursor.remaining()
        );
        let pos_before = cursor.position();

        let length = Varint::from_cursor(cursor).context("failed to parse length")?;
        println!(">>> Record, pos after length: {}", cursor.position());
        let row_id = Varint::from_cursor(cursor).context("failed to parse row id")?;
        println!(">>> Record, pos after row_id: {}", cursor.position());

        let header_bytes_count =
            Varint::from_cursor(cursor).context("failed to parse record header length")?;
        println!(
            ">>> length={length:?}, row_id={row_id:?}, header_bytes_count={header_bytes_count:?}",
        );
        let header_length = header_bytes_count.decoded_value - 1;

        let mut headers = vec![];
        let mut consumed_bytes_count = 0;
        loop {
            if consumed_bytes_count == header_length {
                break;
            } else if consumed_bytes_count > header_length {
                panic!(
                    "consumed more bytes than expected: expected {}, actually {}",
                    header_length, consumed_bytes_count
                );
            }
            let header =
                Varint::from_cursor(cursor).with_context(|| format!("failed to parse header"))?;
            consumed_bytes_count += header.consumed_bytes_count;
            headers.push(header);
        }

        let mut payload = vec![];
        for header in headers.iter() {
            let mut buf = vec![0u8; header.decoded_value as usize];
            cursor.read_exact(&mut buf)?;
            payload.push(buf);
        }

        let pos_after = cursor.position();
        if (pos_after - pos_before) as u32 != length.decoded_value {
            bail!(
                "parsed cell length not match, expeced {}, got {}",
                length.decoded_value,
                pos_after - pos_before
            )
        }

        Ok(Self {
            length,
            row_id,
            header_bytes_count,
            headers,
            payload,
        })
    }
}

#[derive(Debug)]
pub enum VarintCodec {
    /// Value is a NULL.
    S0,

    /// Value is an 8-bit twos-complement integer.
    S1,

    /// Value is a big-endian 16-bit twos-complement integer.
    S2,

    /// Value is a big-endian 24-bit twos-complement integer.
    S3,

    /// Value is a big-endian 32-bit twos-complement integer.
    S4,

    /// Value is a big-endian 48-bit twos-complement integer.
    S5,

    /// Value is a big-endian 64-bit twos-complement integer.
    S6,

    /// Value is a big-endian IEEE 754-2008 64-bit floating point number.
    S7,

    /// Value is the integer 0. (Only available for schema format 4 and higher.)
    S8,

    ///  Value is the integer 1. (Only available for schema format 4 and higher.)
    S9,

    /// Reserved for internal use
    S10_11,

    /// Value is a BLOB that is (N-12)/2 bytes in length.
    S12Even,

    /// Value is a string in the text encoding and (N-13)/2 bytes in length. The nul terminator is not stored.
    S13Odd,
}

impl VarintCodec {
    fn produce(serial_type: u32) -> (Self, u32) {
        match serial_type {
            0 => (Self::S0, 0),
            1 => (Self::S1, 1),
            2 => (Self::S2, 2),
            3 => (Self::S3, 3),
            4 => (Self::S4, 4),
            5 => (Self::S5, 6),
            6 => (Self::S6, 8),
            7 => (Self::S7, 8),
            8 => (Self::S8, 0),
            9 => (Self::S9, 0),
            10 | 11 => unreachable!("table contains reserved record format codec {serial_type}"),
            v => {
                if v % 2 == 0 {
                    (Self::S12Even, (v - 12) / 2)
                } else {
                    (Self::S13Odd, (v - 13) / 2)
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Varint {
    /// Codec of the varint.
    codec: VarintCodec,

    /// The cauculated value before decoding.
    encoded_value: u32,

    /// The decoded value, indicating the size of corresponding data.
    decoded_value: u32,

    /// Count of consumed bytes.
    consumed_bytes_count: u32,
}

impl Varint {
    pub fn from_cursor(cursor: &mut Cursor<&'_ [u8]>) -> Result<Self> {
        // Read bytes till the current byte has 0 on the highest bit.
        //
        // Till then, to get the value: all bytes (after setting highset bit to 0)
        //
        // 1. Remove higheset bytes.
        // 2. Concat values together, early read ones on the high bits side.
        //
        // e.g. For `0x81 0x47`
        //
        // 1. `0x81` is `10000001` with highest bit `1`, indicating it's not the last byte, continue reading.
        // 2. `0x47` is `01000111` with highest bit `0`, indicating it is the last byte, stop reading.
        // 3. Concat bytes with highest bit set to 0: `0000001 <concat> 1000111` -> `11000111` -> `199.`
        let mut raw_bits = vec![];
        let mut consumed_bytes_count = 0;
        loop {
            let curr_byte = cursor.get_u8();
            println!(">>> curr_byte={:x} {}", curr_byte, cursor.position());
            let is_last_byte = if (curr_byte & 0b10000000u8) == 0 {
                true
            } else {
                false
            };
            consumed_bytes_count += 1;
            // TODO: Should drop the first bit of last byte?
            for bit in format!("{:b}", curr_byte).chars().skip(1).map(|x| x as u32) {
                raw_bits.push(bit);
            }
            if is_last_byte {
                break;
            }
        }
        let encoded_value = raw_bits
            .into_iter()
            .rev()
            .enumerate()
            .map(|(idx, x)| x * 2u32.pow(idx as u32))
            .fold(0, |acc, x| acc + x);

        let (codec, decoded_value) = VarintCodec::produce(encoded_value);

        Ok(Self {
            codec,
            encoded_value,
            decoded_value,
            consumed_bytes_count,
        })
    }
}

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            let mut file_bytes = vec![];
            file.read_to_end(&mut file_bytes)?;
            let mut cursor = Cursor::new(file_bytes.as_slice());
            let schema_table =
                SchemaTable::from_cursor(&mut cursor).context("failed to parse schema table")?;
            println!("database page size: {}", schema_table.db_header.page_size);
            println!("number of tables: {}", schema_table.page_header.cells_count);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
