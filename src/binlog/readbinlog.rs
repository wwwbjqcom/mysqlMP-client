/*
@author: xiao cai niao
@datetime: 2019/9/25
*/

use crate::{Config};
use crate::binlog::{readevent, parsevalue};
use crate::binlog::readevent::{InitValue, EventHeader, Tell};
use std::io::{Cursor, Read};
use std::collections::HashMap;
use std::io::BufReader;
use std::fs::File;
use crate::binlog::rollback;
use crate::binlog::getsql;
use std::sync::Arc;
use std::error::Error;
use serde::Serialize;


#[derive(Debug, Clone, Serialize)]
pub enum Traction{
    GtidEvent(readevent::GtidEvent),
    QueryEvent(readevent::QueryEvent),
    TableMapEvent(readevent::TableMap),
    RowEvent(readevent::BinlogEvent,parsevalue::RowValue),
    XidEvent(readevent::XidEvent),
    RotateLogEvent(readevent::RotateLog),
    RowEventStatic{type_code: readevent::BinlogEvent,count: usize},
    Unknown,
}

#[derive(Debug, Clone, Serialize)]
pub struct TractionValue{
    pub event: Vec<Traction>,
    pub cur_sql: Vec<String>,
    pub rollback_sql: Vec<String>,
}
impl TractionValue{
    fn new() -> TractionValue{
        TractionValue{
            event: vec![],
            cur_sql: vec![],
            rollback_sql: vec![]
        }
    }
    fn init(&mut self) {
        self.event = vec![];
        self.cur_sql = vec![];
        self.rollback_sql = vec![];
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RowsSql {
    pub sqls: Vec<TractionValue>,
    pub error: String
}
impl RowsSql{
    fn new() -> RowsSql{
        RowsSql{
            sqls: vec![],
            error: "".to_string()
        }
    }
    fn init(&mut self) {
        self.sqls = vec![];
        self.error = "".to_string();
    }
}


//从文件读取binlog
pub fn readbinlog_fromfile(conf: &Arc<Config>, reader: &mut BufReader<File>, reader_size: u64) -> Result<RowsSql, Box<dyn Error>> {
    //
    let version = crate::meta::get_version(conf)?;
    let reader_size = reader_size;
    //
    let mut traction_value = TractionValue::new();

    let mut row_sql = RowsSql::new();
    let mut tabl_map = readevent::TableMap::new();
    let mut table_cols_info: HashMap<String, Vec<HashMap<String, String>>> = HashMap::new();
    let mut db_tbl = String::from("");

    //

    'all: loop {
        let mut rollback_buf = vec![];
        let mut header_buf = vec![0u8; 19];
        let cur_tell = reader.tell().unwrap();
        if cur_tell + 19 > reader_size {
            return Ok(row_sql);
        }

        reader.read_exact(header_buf.as_mut()).unwrap_or_else(|err|{
            println!("{}",err);
            std::process::exit(1);
        });
        rollback_buf.extend(header_buf.clone());
        let mut cur = Cursor::new(header_buf);
        let event_header: EventHeader = readevent::InitHeader::new(&mut cur);
        //println!("{:?}",event_header);
        let payload = event_header.event_length as usize - event_header.header_length as usize;
        let mut payload_buf = vec![0u8; payload];

        reader.read_exact(payload_buf.as_mut()).unwrap();
        rollback_buf.extend(payload_buf.clone());
        let mut cur = Cursor::new(payload_buf);

        let mut data = Traction::Unknown;
        match event_header.type_code {
            readevent::BinlogEvent::GtidEvent => {

                let v = readevent::GtidEvent::read_event( &event_header, &mut cur, &version);
                data = Traction::GtidEvent(v);
            },
            readevent::BinlogEvent::QueryEvent => {
                let v = readevent::QueryEvent::read_event( &event_header, &mut cur, &version);
                data = Traction::QueryEvent(v);
            },
            readevent::BinlogEvent::TableMapEvent => {
                let v = readevent::TableMap::read_event( &event_header, &mut cur, &version);
                db_tbl = format!("{}.{}", v.database_name, v.table_name).clone();
                let state = crate::meta::get_col(conf, &v.database_name, &v.table_name, &mut table_cols_info);
                match state {
                    Ok(_t) => {}
                    Err(e) => {
                        println!("{:?}", e);
                        row_sql.init();
                        row_sql.error = (*e.to_string()).parse().unwrap();
                        break 'all;
                    }
                }
                tabl_map = v.clone();
                data = Traction::TableMapEvent(v);
            },
            readevent::BinlogEvent::UpdateEvent |
            readevent::BinlogEvent::DeleteEvent |
            readevent::BinlogEvent::WriteEvent => {
                let read_type = crate::meta::ReadType::File;
                let v = parsevalue::RowValue::read_row_value(&mut cur, &tabl_map, &event_header,&read_type);
                //data = Traction::RowEvent(event_header.type_code.clone(),v);
                let cur_sql = getsql::get_command(&v, &event_header.type_code, &mut table_cols_info, &db_tbl,&tabl_map);
                match cur_sql {
                    Ok(t) => {
                        traction_value.cur_sql = t;
                    }
                    Err(e) => {
                        row_sql.init();
                        row_sql.error = (*e.to_string()).parse().unwrap();
                        break 'all;
                    }
                }

                //生成回滚sql
                let r = rollback::rollback_row_event(&rollback_buf, &event_header, &tabl_map);
                let mut r_cur = Cursor::new(r);
                let event_header: EventHeader = readevent::InitHeader::new(&mut cur);
                let v = parsevalue::RowValue::read_row_value(&mut r_cur, &tabl_map, &event_header,&read_type);
                //rollback_data = Traction::RowEvent(event_header.type_code.clone(), v);
                let cur_sql = getsql::get_command(&v, &event_header.type_code, &mut table_cols_info, &db_tbl,&tabl_map);
                match cur_sql {
                    Ok(t) => {
                        traction_value.rollback_sql = t;
                    }
                    Err(e) => {
                        row_sql.init();
                        row_sql.error = (*e.to_string()).parse().unwrap();
                        break 'all;
                    }
                }

            },
            readevent::BinlogEvent::XidEvent => {
                data = Traction::XidEvent(readevent::XidEvent::read_event(&event_header,&mut cur, &version));
                traction_value.event.push(data);
                row_sql.sqls.push(traction_value.clone());
                traction_value.init();
                continue 'all;
            },
            readevent::BinlogEvent::XAPREPARELOGEVENT => {},
            readevent::BinlogEvent::UNKNOWNEVENT => {
                continue 'all;
            }
            readevent::BinlogEvent::RotateLogEvent => {
                data = Traction::RotateLogEvent(readevent::RotateLog::read_event(&event_header, &mut cur, &version));
            }
            _ => {}
        }
        traction_value.event.push(data);

    }
    return Ok(row_sql);
}



