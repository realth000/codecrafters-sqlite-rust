use anyhow::{bail, Context, Ok, Result};
use bytes::Buf;
use regex::Regex;
use std::fs::File;
use std::io::{prelude::*, Cursor, SeekFrom};

/// The 100 bytes database header at the begining of database file.
/// Also and only parts of the first table - The schema table.
#[derive(Debug)]
pub struct DatabaseHeader {
    /// Offset=16, size=2
    ///
    /// The database page size in bytes.
    /// Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
    page_size: u16,

    /// Offset=20, size=1
    ///
    /// The size of reserved region in each B-tree page, usually 20.
    reserved_region_size: u8,
}

impl DatabaseHeader {
    pub fn from_cursor(cursor: &mut Cursor<&'_ [u8]>) -> Result<Self> {
        if cursor.position() != 0 {
            bail!("failed to read database header: expected 0 offset on cursor")
        }

        cursor
            .seek_relative(16)
            .context("page_size position not found")?;
        let page_size = cursor.get_u16();

        cursor
            .seek_relative(3)
            .context("reserved_region_size not found")?;
        let reserved_region_size = cursor.get_u8();

        cursor
            .seek(SeekFrom::Start(100))
            .context("failed to complete db header")?;

        Ok(Self {
            page_size,
            reserved_region_size,
        })
    }
}

/// Type of B-tree page.
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
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
        let page_type = BTreePageType::try_from(cursor.get_u8()).context("invalid B-tree type")?;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableType {
    Table,
    Index,
    View,
    Trigger,
}

impl TryFrom<&[u8]> for TableType {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
        match value {
            b"table" => Ok(Self::Table),
            b"index" => Ok(Self::Index),
            b"view" => Ok(Self::View),
            b"trigger" => Ok(Self::Trigger),
            v => bail!("unknown table type bytes {v:?}"),
        }
    }
}

/// Describe tables.
///
/// This struct is only used in schema table to descibe all tables in the database.
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Table type.
    ///
    /// In this challenge, all tables are `TableType::Table`.
    pub table_type: TableType,

    /// Name of current table.
    pub name: String,

    /// Name of the related table.
    ///
    /// * For index, triggers and views, this field is the name of table which is working on.
    /// * For normal tables, this field is the same as `name`, simply the table name.
    pub table_name: String,

    /// The page number of the root B-tree page for tables and indexes.
    ///
    /// For views, triggers, and virtual tables, the rootpage column is 0 or NULL.
    pub root_page: u32,

    /// SQL text that describes the object.
    pub sql: String,
}

impl TableSchema {
    /// Build from bytes of data.
    ///
    /// The data are grouped in `Vec<u8>` in the fields' order, that is:
    ///
    /// * `data[0]` stores `table_type`.
    /// * `data[1]` stores `name`.
    /// * `data[2]` stores `table_name`.
    /// * `data[3]` stores `root_page`.
    /// * `data[4]` stores `sql`.
    fn from_bytes(data: &Vec<Vec<u8>>) -> Result<Self> {
        if data.len() != 5 {
            bail!(
                "incorrect table description data column size: {}",
                data.len()
            )
        }

        let table_type = TableType::try_from(data[0].as_slice()).context("invalid table type")?;
        let name = String::from_utf8(data[1].clone())
            .with_context(|| format!("invalid name bytes: {:?}", data[1]))?;

        let table_name = String::from_utf8(data[2].clone())
            .with_context(|| format!("invalid table name bytes: {:?}", data[2]))?;

        if data[3].len() > 4 {
            bail!("too many bytes for root page number: {}", data[3].len())
        }

        let root_page = {
            let mut buf = [0u8; 4];
            let mut j = 3;
            for i in (0..data[3].len()).rev() {
                buf[j] = data[3][i].clone();
                j -= 1;
            }
            u32::from_be_bytes(buf)
        };

        let sql = String::from_utf8(data[4].clone())
            .with_context(|| format!("invalid sql bytes: {:?}", data[4]))?
            .replace(['\n', '\t'], "");

        Ok(Self {
            table_type,
            name,
            table_name,
            root_page,
            sql,
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
    ///
    /// Offset of nth table is saved as the nth element in `cells_offsets`.
    cells: Vec<TableSchema>,
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

        let mut cells = vec![];
        for pos in cells_offsets.iter() {
            cursor.set_position(*pos as u64);
            // Parse record, the schema of schema table info.
            let record =
                TableLeafRecord::from_cursor(cursor).context("failed to parse page cell")?;
            if record.headers.len() != 5 {
                bail!(
                    "expected 5 columns in schema table, only got {}",
                    record.headers.len()
                )
            }
            let table_def = TableSchema::from_bytes(&record.payload)
                .context("failed to build table definition")?;
            cells.push(table_def);
        }

        Ok(Self {
            db_header,
            page_header,
            cells_offsets,
            cells,
        })
    }

    pub fn table_names(&self) -> Vec<&str> {
        let mut table_names = vec![];
        for table in self.cells.iter() {
            table_names.push(table.table_name.as_str());
        }
        table_names
    }

    pub fn get_table(&self, table_name: &str) -> Result<&TableSchema> {
        let table = self
            .cells
            .iter()
            .find(|x| x.table_type == TableType::Table && x.table_name == table_name)
            .with_context(|| format!("table {table_name} not found"))?;

        Ok(table)
    }
}

/// B-tree in sqlite3.
#[derive(Debug, Clone)]
pub struct BTreePage {
    /// Page header.
    header: BTreePageHeader,

    /// Body data in the page.
    cells: Cells,
}

impl BTreePage {
    pub fn from_cursor(cursor: &mut Cursor<&'_ [u8]>) -> Result<Self> {
        let initial_pos = cursor.position();
        let header = BTreePageHeader::from_cursor(cursor).context("failed to build page header")?;
        let cells =
            Cells::new(cursor, &header, initial_pos).context("failed to build B-tree page")?;
        Ok(Self { header, cells })
    }
}

#[derive(Debug, Clone)]
pub enum Cells {
    TableLeaf(Vec<TableLeafRecord>),
    TableInterior(Vec<TableInteriorRecord>),
    IndexLeaf(Vec<IndexLeafRecord>),
    IndexInterior(Vec<IndexInteriorRecord>),
}

impl Cells {
    /// Build a list of `Cells` from `cursor` with info provided by `page_header`.
    ///
    /// `initial_pos` is the offset of begining of current page (before parsing page header).
    fn new(
        cursor: &mut Cursor<&'_ [u8]>,
        page_header: &BTreePageHeader,
        initial_pos: u64,
    ) -> Result<Self> {
        let mut cells_offsets = vec![];
        let cells_count = page_header.cells_count;
        for _ in 0..cells_count {
            cells_offsets.push(cursor.get_u16());
        }
        match page_header.page_type {
            BTreePageType::InteriorIndex => {
                println!(">>> interior index offset=0x{:0>8x}", cursor.position());
                let mut cells = vec![];
                for pos in cells_offsets.iter() {
                    let cell_pos = initial_pos + (*pos as u64);
                    cursor.set_position(cell_pos);
                    // Parse record, the schema of schema table info.
                    let record = IndexInteriorRecord::from_cursor(cursor).with_context(|| {
                        format!("failed to build index interior record at 0x{cell_pos:0>8x}")
                    })?;
                    cells.push(record);
                }
                println!(">>> interior index cells: count={}", cells.len());
                for cell in cells.iter() {
                    println!(">>>   left_child_offset=0x{:0>8x}", cell.left_child_offset);
                    println!(">>>   length={}", cell.length.encoded_value);
                    println!(
                        ">>>   header_bytes_count={}",
                        cell.header_bytes_count.encoded_value
                    );
                    for (idx, header) in cell.headers.iter().enumerate() {
                        println!(
                            ">>>     row_id={}, header: {}({:?})",
                            cell.row_id.encoded_value,
                            header
                                .codec()
                                .unwrap()
                                .decode_to_string(&cell.payload[idx])
                                .unwrap(),
                            header.codec()
                        );
                    }
                }

                Ok(Self::IndexInterior(cells))
            }
            BTreePageType::InteriorTable => {
                let mut cells = vec![];
                for pos in cells_offsets.iter() {
                    let cell_pos = initial_pos + (*pos as u64);
                    cursor.set_position(cell_pos);
                    // Parse record, the schema of schema table info.
                    let record = TableInteriorRecord::from_cursor(cursor).with_context(|| {
                        format!("failed to build table interior record at 0x{cell_pos:0>8x}")
                    })?;
                    cells.push(record);
                }
                println!(">>> interior table cells: {cells:?}");

                Ok(Self::TableInterior(cells))
            }
            BTreePageType::LeafIndex => {
                println!(">>> leaf index offset={}", cursor.position());
                let mut cells = vec![];
                for pos in cells_offsets.iter() {
                    let cell_pos = initial_pos + (*pos as u64);
                    cursor.set_position(cell_pos);
                    // Parse record, the schema of schema table info.
                    let record = IndexLeafRecord::from_cursor(cursor).with_context(|| {
                        format!("failed to build index leaf record at 0x{cell_pos:0>8x}")
                    })?;
                    cells.push(record);
                }

                Ok(Self::IndexLeaf(cells))
            }
            BTreePageType::LeafTable => {
                let mut cells = vec![];
                for pos in cells_offsets.iter() {
                    let cell_pos = initial_pos + (*pos as u64);
                    cursor.set_position(cell_pos);
                    // Parse record, the schema of schema table info.
                    let record = TableLeafRecord::from_cursor(cursor)
                        .context("failed to parse page cell")?;
                    cells.push(record);
                }

                Ok(Self::TableLeaf(cells))
            }
        }
    }
}

/// Table data in database.
///
/// This type is NOT the schema of table in `SchemaTable`.
/// For the definition of table, see `SchemaTable`.
///
/// Compared to the `SchemaTable`, `Table` does not have database header, all other fields are the same.
///
/// Table can be stored in a single page or multiple pages, till not we assume small enough to store in
/// a single page.
#[derive(Debug, Clone)]
pub struct Table {
    /// Table data.
    ///
    /// Offset of nth table is saved as the nth element in `cells_offsets`.
    cells: Vec<TableLeafRecord>,
}

impl Table {
    pub fn from_cursor(cursor: &mut Cursor<&'_ [u8]>, page_size: u32) -> Result<Self> {
        let page = BTreePage::from_cursor(cursor).context("failed to build B-tree page")?;

        fn load_page(
            cursor: &Cursor<&'_ [u8]>,
            page_idx: u32,
            page_size: u32,
        ) -> Result<BTreePage> {
            let mut cursor = cursor.clone();
            let table_start_pos = (page_size as u64) * (page_idx as u64 - 1);
            cursor.set_position(table_start_pos);
            BTreePage::from_cursor(&mut cursor).with_context(|| {
                format!(
                    "failed to load B-tree page id={page_idx} at offset=0x{table_start_pos:0>8x}"
                )
            })
        }

        fn traverse_page(
            cursor: &Cursor<&'_ [u8]>,
            page: BTreePage,
            page_size: u32,
        ) -> Result<Vec<TableLeafRecord>> {
            match page.cells {
                Cells::TableLeaf(v) => Ok(v),
                Cells::TableInterior(v) => {
                    let mut records = vec![];
                    for page in v.into_iter() {
                        let p = load_page(cursor, page.next_page, page_size)
                            .context("failed to load page")?;
                        let mut records_in_page = traverse_page(cursor, p, page_size)?;
                        records.append(&mut records_in_page);
                    }
                    Ok(records)
                }
                _ => Ok(vec![]),
            }
        }

        let cells = traverse_page(&cursor, page, page_size).context("failed to traverse page")?;

        Ok(Self { cells })
    }

    /// Get all data in one column specified by column id.
    ///
    /// Note that the data in column is converted to String.
    pub fn get_columns(
        &self,
        selected_cols: &[&ColumnInfo],
        cols: &[ColumnInfo],
        where_clause: Option<WhereClause>,
    ) -> Result<Vec<String>> {
        if self.cells.is_empty() {
            return Ok(vec![]);
        }

        fn from_number(data: &[u8]) -> String {
            let mut level = 0;
            let mut acc: u64 = 0;
            for i in data.iter().rev() {
                acc += (i.to_owned() as u64) * 2_u64.pow(level);
                level += 1;
            }
            format!("{acc}")
        }

        fn from_text(data: &[u8]) -> String {
            String::from_utf8(data.to_owned()).unwrap()
        }

        // Find the info about the column in `WHERE` clause.
        let wh = if let Some(wh) = &where_clause {
            match cols.iter().position(|x| x.name == wh.col_name) {
                Some(pos) => Some((pos, wh.col_value)),
                None => bail!(
                    "column \"{}\" in where clause not found in table",
                    wh.col_name
                ),
            }
        } else {
            None
        };

        let mut data = vec![];

        for cell in self.cells.iter() {
            let mut row_result = vec![];

            // If we have a `WHERE` clause, check the current cell's column value hits the value
            // in `WHERE` clause or not, skip current cell if not.
            if let Some(wh) = wh {
                let wh_target = match cols[wh.0].ty {
                    ColumnType::Integer => from_number(&cell.payload[wh.0]),
                    ColumnType::Text => from_text(&cell.payload[wh.0]),
                };
                if wh_target != wh.1 {
                    continue;
                }
            }

            for col in selected_cols {
                let d = match col.ty {
                    ColumnType::Integer => from_number(&cell.payload[col.idx]),
                    ColumnType::Text => from_text(&cell.payload[col.idx]),
                };
                // A zero id means autoincrement.
                // We should handle AUTOINCREMENT fields more carefully, but it's fine.
                if col.name == "id" && d == "0" {
                    row_result.push(cell.row_id.encoded_value.to_string());
                } else {
                    row_result.push(d);
                }
            }
            data.push(row_result.join("|"));
        }

        Ok(data)
    }
}

/// Record in cell, carryig data, for table B-tree page that is a leaf page.
///
/// Contains header and payload where header is `varint`s and payload is `Vec<u8>`s.
///
/// Each record holds a row of data in the table, stores in `payload` field.
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
#[derive(Debug, Clone)]
pub struct TableLeafRecord {
    /// Cell length, totally.
    ///
    /// The value represents bytes count of current `Cell`, including `length` field itself.
    length: Varint,

    /// Row id.
    row_id: Varint,

    /// Size of the record header.
    header_bytes_count: Varint,

    /// Headers.
    ///
    /// Headers describes how many payload it have and where these payloads are.
    /// Always have a same length as `payload`.
    headers: Vec<Varint>,

    /// Payload.
    ///
    /// Always have a same length as `headers`.
    ///
    /// Each payload in holds one column data in one row, that is, all payload in
    /// the same record forms an entire row of the table.
    payload: Vec<Vec<u8>>,
}

impl TableLeafRecord {
    pub fn from_cursor(cursor: &mut Cursor<&'_ [u8]>) -> Result<Self> {
        if !cursor.has_remaining() {
            panic!("cursor ended");
        }

        // Sum of bytes the header of record consumes.
        let record_bytes_count = Varint::from_cursor(cursor).context("failed to parse length")?;
        let row_id = Varint::from_cursor(cursor).context("failed to parse row id")?;

        /* Parsing record header */

        let pos_before = cursor.position();
        let header_bytes_count =
            Varint::from_cursor(cursor).context("failed to parse record header length")?;

        // Sum of bytes all column info consumes.
        // Value includes header_bytes_count ifself so the remaining bytes for columns to consume is one byte less.
        let column_bytes_count = header_bytes_count.encoded_value - 1;

        let mut headers = vec![];
        let mut consumed_bytes_count = 0;
        loop {
            if consumed_bytes_count == column_bytes_count {
                break;
            } else if consumed_bytes_count > column_bytes_count {
                panic!(
                    "consumed more bytes than expected: expected {}, actually {}",
                    column_bytes_count, consumed_bytes_count
                );
            }
            let header =
                Varint::from_cursor(cursor).with_context(|| format!("failed to parse header"))?;
            consumed_bytes_count += header.consumed_bytes_count;
            headers.push(header);
        }

        let mut payload = vec![];
        for header in headers.iter() {
            let mut buf = vec![0u8; header.decoded()? as usize];
            cursor
                .read_exact(&mut buf)
                .with_context(|| format!("when fill header size {}", buf.len()))?;
            payload.push(buf);
        }

        let pos_after = cursor.position();
        if (pos_after - pos_before) as u32 != record_bytes_count.encoded_value {
            bail!(
                "parsed table leaf record length not match, expeced {}, got {}",
                record_bytes_count.encoded_value,
                pos_after - pos_before
            )
        }

        Ok(Self {
            length: record_bytes_count,
            row_id,
            header_bytes_count,
            headers,
            payload,
        })
    }
}

/// Record for table interior page.
///
/// Carries no table data but the page id of more B-tree pages for current table.
/// The carrying may be recursive, means interior record points to more interior pages
/// and after more pointing rounds there are leafe pages that carries data.
#[derive(Debug, Clone)]
pub struct TableInteriorRecord {
    /// Page id of next page.
    ///
    /// A 4-byte big-endian page number which is the left child pointer.
    next_page: u32,

    /// Row id.
    row_id: Varint,
}

impl TableInteriorRecord {
    pub fn from_cursor(cursor: &mut Cursor<&'_ [u8]>) -> Result<Self> {
        if !cursor.has_remaining() {
            bail!("no data left for table iterior record")
        }

        let next_page = cursor.get_u32();
        let row_id = Varint::from_cursor(cursor)
            .context("failed to build row_id of table interior record")?;

        Ok(Self { next_page, row_id })
    }
}

#[derive(Debug, Clone)]
pub struct IndexLeafRecord {
    /// Cell length, totally.
    ///
    /// The value represents bytes count of current `Cell`, including `length` field itself.
    length: Varint,

    /// Size of the record header.
    header_bytes_count: Varint,

    /// Headers.
    ///
    /// Headers describes how many payload it have and where these payloads are.
    /// Always have a same length as `payload`.
    headers: Vec<Varint>,

    /// Payload.
    ///
    /// Always have a same length as `headers`.
    ///
    /// Each payload in holds one column data in one row, that is, all payload in
    /// the same record forms an entire row of the table.
    payload: Vec<Vec<u8>>,

    /// Row id placed after payload
    ///
    /// This is also part of `payload` but not "keys"
    row_id: Varint,
}

impl IndexLeafRecord {
    pub fn from_cursor(cursor: &mut Cursor<&'_ [u8]>) -> Result<Self> {
        if !cursor.has_remaining() {
            panic!("cursor ended");
        }

        // Sum of bytes the header of record consumes.
        let record_bytes_count = Varint::from_cursor(cursor).context("failed to parse length")?;

        /* Parsing record header */

        let pos_before = cursor.position();
        let header_bytes_count =
            Varint::from_cursor(cursor).context("failed to parse record header length")?;

        // Sum of bytes all column info consumes.
        // Value includes header_bytes_count ifself so the remaining bytes for columns to consume is one byte less.
        let column_bytes_count = header_bytes_count.encoded_value - 1;

        let mut headers = vec![];
        let mut consumed_bytes_count = 0;
        loop {
            if consumed_bytes_count == column_bytes_count {
                break;
            } else if consumed_bytes_count > column_bytes_count {
                panic!(
                    "consumed more bytes than expected: expected {}, actually {}",
                    column_bytes_count, consumed_bytes_count
                );
            }
            let header =
                Varint::from_cursor(cursor).with_context(|| format!("failed to parse header"))?;
            consumed_bytes_count += header.consumed_bytes_count;
            headers.push(header);
        }

        let mut payload = vec![];
        for header in headers.iter() {
            let mut buf = vec![0u8; header.decoded()? as usize];
            cursor
                .read_exact(&mut buf)
                .with_context(|| format!("when fill header size {}", buf.len()))?;
            payload.push(buf);
        }
        let row_id = Varint::from_cursor(cursor)
            .context("failed to parse row_id varint in index leaf page")?;

        let pos_after = cursor.position();
        if (pos_after - pos_before) as u32 != record_bytes_count.encoded_value {
            bail!(
                "parsed index leaf record length not match, expeced {}, got {}",
                record_bytes_count.encoded_value,
                pos_after - pos_before
            )
        }

        Ok(Self {
            length: record_bytes_count,
            header_bytes_count,
            headers,
            payload,
            row_id,
        })
    }
}

#[derive(Debug, Clone)]
pub struct IndexInteriorRecord {
    /// Offset of the left most child.
    left_child_offset: u32,

    /// Cell length, totally.
    ///
    /// The value represents bytes count of current `Cell`, including `length` field itself.
    length: Varint,

    /// Size of the record header.
    header_bytes_count: Varint,

    /// Headers.
    ///
    /// Headers describes how many payload it have and where these payloads are.
    /// Always have a same length as `payload`.
    headers: Vec<Varint>,

    /// Payload.
    ///
    /// Always have a same length as `headers`.
    ///
    /// Each payload in holds one column data in one row, that is, all payload in
    /// the same record forms an entire row of the table.
    payload: Vec<Vec<u8>>,

    /// Row id placed after payload
    ///
    /// This is also part of `payload` but not "keys"
    row_id: Varint,
}

impl IndexInteriorRecord {
    fn from_cursor(cursor: &mut Cursor<&'_ [u8]>) -> Result<Self> {
        if !cursor.has_remaining() {
            panic!("cursor ended");
        }

        let left_child_offset = cursor.get_u32();

        // Sum of bytes the header of record consumes.
        let record_bytes_count = Varint::from_cursor(cursor).context("failed to parse length")?;

        /* Parsing record header */

        let pos_before = cursor.position();
        let header_bytes_count =
            Varint::from_cursor(cursor).context("failed to parse record header length")?;

        // Sum of bytes all column info consumes.
        // Value includes header_bytes_count ifself so the remaining bytes for columns to consume is one byte less.
        let column_bytes_count = header_bytes_count.encoded_value - 1;

        let mut headers = vec![];
        let mut consumed_bytes_count = 0;
        loop {
            if consumed_bytes_count == column_bytes_count {
                break;
            } else if consumed_bytes_count > column_bytes_count {
                panic!(
                    "consumed more bytes than expected: expected {}, actually {}",
                    column_bytes_count, consumed_bytes_count
                );
            }
            let header =
                Varint::from_cursor(cursor).with_context(|| format!("failed to parse header"))?;
            consumed_bytes_count += header.consumed_bytes_count;
            headers.push(header);
        }

        let mut payload = vec![];
        for header in headers.iter() {
            let mut buf = vec![0u8; header.decoded()? as usize];
            cursor
                .read_exact(&mut buf)
                .with_context(|| format!("when fill header size {}", buf.len()))?;
            payload.push(buf);
        }
        let row_id = Varint::from_cursor(cursor)
            .context("failed to parse row_id varint in index leaf page")?;

        let pos_after = cursor.position();
        if (pos_after - pos_before) as u32 != record_bytes_count.encoded_value + 1 {
            bail!(
                "parsed index interior record length not match, expeced {}, got {}",
                record_bytes_count.encoded_value,
                pos_after - pos_before
            )
        }

        Ok(Self {
            left_child_offset,
            length: record_bytes_count,
            header_bytes_count,
            headers,
            payload,
            row_id,
        })
    }
}

#[derive(Debug, Clone)]
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
    fn produce(serial_type: u32) -> Result<(Self, u32)> {
        match serial_type {
            0 => Ok((Self::S0, 0)),
            1 => Ok((Self::S1, 1)),
            2 => Ok((Self::S2, 2)),
            3 => Ok((Self::S3, 3)),
            4 => Ok((Self::S4, 4)),
            5 => Ok((Self::S5, 6)),
            6 => Ok((Self::S6, 8)),
            7 => Ok((Self::S7, 8)),
            8 => Ok((Self::S8, 0)),
            9 => Ok((Self::S9, 0)),
            10 | 11 => bail!("table contains reserved record format codec {serial_type}"),
            v => {
                if v % 2 == 0 {
                    Ok((Self::S12Even, (v - 12) / 2))
                } else {
                    Ok((Self::S13Odd, (v - 13) / 2))
                }
            }
        }
    }

    /// Decode the data to string.
    fn decode_to_string(&self, data: &[u8]) -> Result<String> {
        match self {
            Self::S0 => Ok("null".into()),
            Self::S12Even => Ok(format!("{:?}", data)),
            Self::S13Odd => String::from_utf8(data.to_vec()).context("invalid S13Odd data"),
            Self::S1 => Ok(format!("{}", u8::from_be_bytes([data[0]]))),
            Self::S2 => Ok(format!("{}", u16::from_be_bytes([data[0], data[1]]))),
            Self::S3 => Ok(format!(
                "{}",
                u32::from_be_bytes([0, data[0], data[1], data[2]])
            )),
            Self::S4 => Ok(format!(
                "{}",
                u32::from_be_bytes([data[0], data[1], data[2], data[3]])
            )),
            Self::S5 => Ok(format!(
                "{}",
                u64::from_be_bytes([0, 0, 0, data[0], data[1], data[2], data[3], data[4]])
            )),
            v => unimplemented!("unsupported codec {v:?}"),
        }
    }
}

/// A varint stores a single value effciently.
///
/// * If the varint stores column related data, it also represents the column type in `VarintCodec`.
///   * For column type, see comments in `VarintCodec`.
///   * In this usage, decoded value is the column type.
/// * If the varint stores data not related to column (e.g. size of record, row id), the encoded
///   value is the value we want.
#[derive(Debug, Clone)]
pub struct Varint {
    /// The cauculated value before decoding.
    encoded_value: u32,

    /// Count of consumed bytes.
    consumed_bytes_count: u32,

    /// Byte offsets.
    offsets: (u64, u64),
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
        let offset_begin = cursor.position();
        loop {
            let curr_byte = cursor.get_u8();
            let is_last_byte = if (curr_byte & 0b10000000u8) == 0 {
                true
            } else {
                false
            };
            consumed_bytes_count += 1;
            // TODO: Should drop the first bit of last byte?
            for bit in format!("{:0>8b}", curr_byte)
                .chars()
                .skip(1)
                .map(|x| x as u32 - 48)
            {
                raw_bits.push(bit);
            }
            if is_last_byte {
                break;
            }
        }
        let offset_end = cursor.position();

        let encoded_value = raw_bits
            .into_iter()
            .rev()
            .enumerate()
            .map(|(idx, x)| x * 2u32.pow(idx as u32))
            .fold(0, |acc, x| acc + x);

        Ok(Self {
            encoded_value,
            consumed_bytes_count,
            offsets: (offset_begin, offset_end),
        })
    }

    /// Get the decoded value of current varint.
    ///
    /// As not all usage of varint are using the decoded value, only ones recoding data
    /// in table (or say "in the payload of record") are using decoded value (say it "SerialType"),
    /// we no longer decode or store the value in varint when constructing a varint instance, instead
    /// only decode it when needed.
    pub fn decoded(&self) -> Result<u32> {
        let (_codec, decoded_value) =
            VarintCodec::produce(self.encoded_value).context("invalid varint value")?;
        Ok(decoded_value)
    }

    /// Get the codec of current varint.
    ///
    /// See `decoded` for details.
    pub fn codec(&self) -> Result<VarintCodec> {
        let (codec, _) =
            VarintCodec::produce(self.encoded_value).context("invalid varint value")?;
        Ok(codec)
    }
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub idx: usize,
    pub ty: ColumnType,
}

#[derive(Debug, Clone)]
pub struct WhereClause<'a> {
    pub col_name: &'a str,
    pub col_value: &'a str,
}

#[derive(Debug, Clone)]
pub enum ColumnType {
    Integer,
    Text,
}

impl<'a> TryFrom<&'a str> for ColumnType {
    type Error = anyhow::Error;

    fn try_from(value: &'a str) -> std::result::Result<Self, Self::Error> {
        match value {
            "integer" => Ok(Self::Integer),
            "text" => Ok(Self::Text),
            v => bail!("unsupported column type \"{v}\""),
        }
    }
}

fn parse_sql_colomn_names(sql: &str) -> Result<Vec<ColumnInfo>> {
    if !sql.starts_with("CREATE TABLE") {
        bail!("unsupported sql statement: \"{sql}\"")
    }

    let p1 = sql.find('(').context("'(' not found in sql")?;
    let p2 = sql.find(')').context("')' not found in sql")?;

    let rows = sql[p1 + 1..p2]
        .split(",")
        .enumerate()
        .map(|(idx, x)| {
            let x = x.trim();
            if x.starts_with('"') {
                let col_name_end_pos = x.chars().skip(1).position(|x| x == '"').unwrap() + 1;
                let name = x[1..col_name_end_pos].to_string();
                let ty = x[col_name_end_pos + 2..]
                    .split_once(' ')
                    .map_or_else(|| Some(&x[col_name_end_pos + 2..]), |x| Some(x.0))
                    .context("invalid col")
                    .and_then(|x| ColumnType::try_from(x).context("invalid column type"))
                    .unwrap(); // ;
                ColumnInfo { name, idx, ty }
            } else {
                // Support column name with space and quoted: "size range" text.
                let mut it = x.split(" ").take(2);
                let name = it.next().unwrap().to_string();
                let ty = ColumnType::try_from(it.next().unwrap()).unwrap();
                ColumnInfo { name, idx, ty }
            }
        })
        .collect();
    Ok(rows)
}

fn load_schema_from_file(file_path: &str) -> Result<SchemaTable> {
    let mut file = File::open(file_path)?;
    let mut file_bytes = vec![];
    file.read_to_end(&mut file_bytes)?;
    let mut cursor = Cursor::new(file_bytes.as_slice());
    let schema_table =
        SchemaTable::from_cursor(&mut cursor).context("failed to parse schema table")?;
    Ok(schema_table)
}

fn load_table_from_file(file_path: &str, table_name: &str) -> Result<Table> {
    let mut file = File::open(file_path)?;
    let mut file_bytes = vec![];
    file.read_to_end(&mut file_bytes)?;
    let mut cursor = Cursor::new(file_bytes.as_slice());
    let schema_table =
        SchemaTable::from_cursor(&mut cursor).context("failed to parse schema table")?;
    // Command for testing: print all data in table.
    // Count rows in table.
    let target_table = schema_table
        .cells
        .iter()
        .find(|x| x.table_name == table_name)
        .with_context(|| format!("table {table_name} not found"))?;
    let page_size = schema_table.db_header.page_size as u32;
    let offset = (target_table.root_page - 1) * page_size;
    let mut cursor = Cursor::new(file_bytes.as_slice());
    cursor
        .seek_relative(offset as i64)
        .context("failed to find table postion")?;

    let table = Table::from_cursor(&mut cursor, page_size)
        .with_context(|| format!("failed to parse table {}", table_name))?;

    Ok(table)
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
    let file_path = &args[1];
    let command = &args[2];

    let select_column_re =
        Regex::new(r#"(SELECT|select) (?<columns>[\w ,]+) (FROM|from) (?<table>\w+)( ((WHERE|where) (?<col_name>\w+) ?= ?'(?<col_val>.+)'))?$"#)
            .unwrap();

    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(file_path)?;
            let mut file_bytes = vec![];
            file.read_to_end(&mut file_bytes)?;
            let mut cursor = Cursor::new(file_bytes.as_slice());
            let schema_table =
                SchemaTable::from_cursor(&mut cursor).context("failed to parse schema table")?;
            println!("database page size: {}", schema_table.db_header.page_size);
            println!("number of tables: {}", schema_table.page_header.cells_count);
        }
        ".tables" => {
            let mut file = File::open(file_path)?;
            let mut file_bytes = vec![];
            file.read_to_end(&mut file_bytes)?;
            let mut cursor = Cursor::new(file_bytes.as_slice());
            let schema_table =
                SchemaTable::from_cursor(&mut cursor).context("failed to parse schema table")?;
            let mut table_names = schema_table
                .table_names()
                .iter()
                .filter(|x| x != &&"sqlite_sequence")
                .map(|x| x.to_string())
                .collect::<Vec<_>>();
            table_names.sort();
            let table_names = table_names.join(" ");
            println!("{}", table_names);
        }
        v => {
            if v.starts_with("select count(*) from ") {
                // Count rows in table.
                let table_name = v.split_at(21).1;
                let table = load_table_from_file(&file_path, table_name)?;
                println!("{}", table.cells.len());

                return Ok(());
            } else if select_column_re.is_match(v) {
                let cap = select_column_re.captures(v).unwrap();
                let column_names = cap
                    .name("columns")
                    .unwrap()
                    .as_str()
                    .split(",")
                    .map(|x| x.trim().to_string())
                    .collect::<Vec<_>>();
                let table_name = cap.name("table").unwrap().as_str();
                let col_name = cap.name("col_name").map(|x| x.as_str());
                let col_value = cap.name("col_val").map(|x| x.as_str());
                let where_clause = if let (Some(col_name), Some(col_value)) = (col_name, col_value)
                {
                    Some(WhereClause {
                        col_name,
                        col_value,
                    })
                } else {
                    None
                };

                let table = load_table_from_file(&file_path, table_name)?;
                let schema = load_schema_from_file(&file_path)?;
                let table_schema = schema.get_table(table_name)?;
                let sql_columns = parse_sql_colomn_names(table_schema.sql.as_str())?;
                let mut column_idx = vec![];
                for column_name in column_names.iter() {
                    let col_info = sql_columns
                        .iter()
                        .find(|x| x.name.as_str() == column_name)
                        .with_context(|| {
                            format!(
                                "column \"{}\" not found in schema \"{}\"",
                                column_name, table_schema.sql
                            )
                        })?;

                    column_idx.push(col_info);
                }
                let column_datas = table.get_columns(
                    column_idx.as_slice(),
                    sql_columns.as_slice(),
                    where_clause,
                )?;
                if !column_datas.is_empty() {
                    println!("{}", column_datas.join("\n"));
                }
                return Ok(());
            }

            // Testing
            if v.starts_with("__print ") {
                // Command for testing: print all data in table.
                // Count rows in table.
                let table_name = v.split_at(8).1;
                let table = load_table_from_file(&file_path, table_name)?;
                let schema = load_schema_from_file(&file_path)?;
                let table_schema = schema.get_table(table_name)?;
                println!(
                    "columns: {:?}",
                    parse_sql_colomn_names(table_schema.sql.as_str()).unwrap()
                );
                for record in table.cells.iter() {
                    let row_id = &record.row_id.decoded().context("invalid row_id")?;
                    let id_type = &record.headers[0].codec().context("invalid id type")?;
                    let id = &record.payload[0];
                    let name_type = &record.headers[1].codec().context("invalid name type")?;
                    let name = String::from_utf8(record.payload[1].clone()).unwrap();
                    let color_type = &record.headers[2].codec().context("invalid color type")?;
                    let color = String::from_utf8(record.payload[2].clone()).unwrap();
                    println!("row_id={row_id}, id={id:?}({id_type:?}), name={name}({name_type:?}), color={color}({color_type:?})");
                }
                return Ok(());
            }
            bail!("Missing or invalid command passed: {}", command)
        }
    }

    Ok(())
}
