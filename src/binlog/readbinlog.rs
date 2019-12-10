/*
@author: xiao cai niao
@datetime: 2019/9/25
*/

use crate::{Config};
use crate::binlog::{readevent, parsevalue};
use crate::binlog::readevent::{InitValue, EventHeader, Tell};
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::collections::HashMap;
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
    pub error: String,
    pub etype: String,
}
impl RowsSql{
    pub fn new() -> RowsSql{
        RowsSql{
            sqls: vec![],
            error: "".to_string(),
            etype: "".to_string()
        }
    }
    fn init(&mut self) {
        self.sqls = vec![];
        self.error = "".to_string();
        self.etype = "".to_string();
    }

    pub fn set_rollback_etype(&mut self) {
        self.etype = "rollback".to_string();
    }

    pub fn set_append_etype(&mut self) {
        self.etype = "append".to_string();
    }
}


//从文件读取binlog
//pub fn readbinlog_fromfile<R: Read + Seek>(conf: &Arc<Config>, reader: &mut BufReader<File>, reader_size: u64) -> Result<RowsSql, Box<dyn Error>> {
pub fn parse<R: Read + Seek>(conf: &Arc<Config>, reader: &mut R, reader_size: u64, readfile: bool) -> Result<RowsSql, Box<dyn Error>> {
    //
    let version = crate::meta::get_version(conf)?;
    //
    let mut traction_value = TractionValue::new();

    let mut row_sql = RowsSql::new();
    let mut tabl_map = readevent::TableMap::new();
    let mut table_cols_info: HashMap<String, Vec<HashMap<String, String>>> = HashMap::new();
    let mut db_tbl = String::from("");

    //

    'all: loop {
        //info!("start");
        let mut rollback_buf = vec![];
        let mut header_buf = vec![0u8; 19];
        let cur_tell = reader.tell()?;
        if readfile{
            if cur_tell + 19 > reader_size {
                return Ok(row_sql);
            }
        }else {
            if cur_tell >= reader_size {
                return Ok(row_sql);
            }
        }

        //info!("startb");
        reader.read_exact(header_buf.as_mut())?;
        rollback_buf.extend(header_buf.clone());
        let mut cur = Cursor::new(header_buf);
        let event_header: EventHeader = readevent::InitHeader::new(&mut cur);
        info!("event_header: {:?}", event_header);
        let payload = event_header.event_length as usize - event_header.header_length as usize;
        let mut payload_buf = vec![0u8; payload];

        reader.read_exact(payload_buf.as_mut())?;
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
                        info!("{:?}", e);
                        row_sql.init();
                        row_sql.error = (*e.to_string()).parse()?;
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
                //info!("cur_row_value: {:?}", &v);
                //data = Traction::RowEvent(event_header.type_code.clone(),v);
                let cur_sql = getsql::get_command(&v, &event_header.type_code, &mut table_cols_info, &db_tbl,&tabl_map);
                //info!("cur_sql: {:?}", &cur_sql);
                match cur_sql {
                    Ok(t) => {
                        traction_value.cur_sql = t;
                    }
                    Err(e) => {
                        row_sql.init();
                        row_sql.error = (*e.to_string()).parse()?;
                        break 'all;
                    }
                }
                //生成回滚sql
                //info!("rollback_cur_buf:{:?}", &rollback_buf);
                let r = rollback::rollback_row_event(&rollback_buf, &event_header, &tabl_map);
                let mut r_cur = Cursor::new(r);
                let event_header: EventHeader = readevent::InitHeader::new(&mut r_cur);
                //info!("rollback_header:{:?}", &event_header);
                let payload = event_header.event_length as usize - event_header.header_length as usize;
                let mut payload_buf = vec![0u8; payload];
                r_cur.read_exact(payload_buf.as_mut())?;
                //info!("rollback_buf:{:?}", &payload_buf);
                let mut cur = Cursor::new(payload_buf);
                let v = parsevalue::RowValue::read_row_value(&mut cur, &tabl_map, &event_header,&read_type);
                //info!("rollback_row_value: {:?}", &v);
                //rollback_data = Traction::RowEvent(event_header.type_code.clone(), v);
                let cur_sql = getsql::get_command(&v, &event_header.type_code, &mut table_cols_info, &db_tbl,&tabl_map);
                //info!("rollback_row_sql: {:?}", &cur_sql);
                match cur_sql {
                    Ok(t) => {
                        traction_value.rollback_sql = t;
                    }
                    Err(e) => {
                        row_sql.init();
                        row_sql.error = (*e.to_string()).parse()?;
                        break 'all;
                    }
                }
                //

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
                let a = readevent::RotateLog::read_event(&event_header, &mut cur, &version);
                let c_row_sql = rotate_readbinlog(conf, readfile, &a.binlog_file)?;
                for trac_sql in c_row_sql.sqls {
                    row_sql.sqls.push(trac_sql);
                }
                continue 'all;
            }
            _ => {}
        }
        traction_value.event.push(data);

    }
    return Ok(row_sql);
}

fn rotate_readbinlog(conf: &Arc<Config>, readfile: bool, logname: &String) -> Result<RowsSql , Box<dyn Error>> {
    info!("rotate to new log: {}", logname);
    let path = format!("{}/{}",conf.binlogdir,logname);
    let mut r = crate::binlog::open_file(&path)?;
    r.seek(SeekFrom::End(0)).unwrap();
    let r_size = r.tell().unwrap();
    r.seek(SeekFrom::Start(4)).unwrap();
    let r_sqls = parse(conf, &mut r, r_size, readfile)?;
    Ok(r_sqls)
}





