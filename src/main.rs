use anyhow::{bail, Context, Result};
use std::fs::File;
use std::io::prelude::*;

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
    pub fn from_reader(reader: &mut impl Read) -> Result<Self> {
        reader
            .read_exact(&mut [0u8; 16])
            .context("failed to read pre-offset bytes")?;

        let mut page_size_buf = [0u8; 2];
        reader
            .read_exact(&mut page_size_buf)
            .context("failed to read page size")?;
        let page_size = u16::from_be_bytes(page_size_buf);

        reader
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
    pub fn from_reader(reader: &mut impl Read) -> Result<Self> {
        let mut page_type_buf = [0u8; 1];
        reader
            .read_exact(&mut page_type_buf)
            .context("failed to read page type")?;
        let page_type = BTreePageType::try_from(page_type_buf[0])
            .context("failed to parse B-tree page size")?;

        let mut free_block_offset_buf = [0u8; 2];
        reader
            .read_exact(&mut free_block_offset_buf)
            .context("failed to read free block offset")?;
        let free_block_offset = u16::from_be_bytes(free_block_offset_buf);

        let mut cells_count_buf = [0u8; 2];
        reader
            .read_exact(&mut cells_count_buf)
            .context("failed to read cells count")?;
        let cells_count = u16::from_be_bytes(cells_count_buf);

        let total_size = match page_type {
            BTreePageType::InteriorIndex | BTreePageType::InteriorTable => 12,
            BTreePageType::LeafIndex | BTreePageType::LeafTable => 8,
        };

        reader
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
}

impl SchemaTable {
    pub fn from_reader(reader: &mut impl Read) -> Result<Self> {
        let db_header = DatabaseHeader::from_reader(reader).context("failed to parse db header")?;
        let page_header =
            BTreePageHeader::from_reader(reader).context("failed to parse page_header")?;

        Ok(Self {
            db_header,
            page_header,
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
            let schema_table =
                SchemaTable::from_reader(&mut file).context("failed to parse schema table")?;
            println!("database page size: {}", schema_table.db_header.page_size);
            println!("number of tables: {}", schema_table.page_header.cells_count);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
