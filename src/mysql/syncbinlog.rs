/*
@author: xiao cai niao
@datetime: 2019/11/11
*/
use serde::Deserialize;
use serde::Serialize;
use crate::Config;
use std::sync::Arc;
use std::net::TcpStream;
use std::error::Error;
use crate::binlog::open_file;
use std::io::{Seek, SeekFrom, Read, Cursor};
use crate::binlog::readevent::Tell;
use crate::mysql::MyProtocol;

#[derive(Deserialize, Debug)]
pub struct SyncBinlogInfo{
    binlog: String,
    position: usize
}

#[derive(Serialize, Deserialize)]
pub struct BinlogValue{
    value: Vec<u8>
}
impl BinlogValue{
    fn new() -> BinlogValue {
        BinlogValue{value: vec![]}
    }
}

pub fn pull_binlog_info(conf: &Arc<Config>, tcp: &mut TcpStream, buf: &Vec<u8>) -> Result<(), Box<dyn Error>>{
    info!("synchronization difference binlog");
    let sync_info: SyncBinlogInfo = serde_json::from_slice(&buf[9..])?;
    info!("synchronization info: {:?}",&sync_info);
    let path = format!("{}/{}",conf.binlogdir,sync_info.binlog);
    let mut reader = open_file(&path)?;
    reader.seek(SeekFrom::End(0))?;
    let end_pos = reader.tell()?;
    let read_bytes = end_pos - sync_info.position as u64;
    let mut binlog_value = BinlogValue::new();
    if read_bytes > 0{
        reader.seek(SeekFrom::Start(sync_info.position as u64))?;
        reader.read_to_end(&mut binlog_value.value)?;
    }
    crate::mysql::send_value_packet(tcp, &binlog_value, MyProtocol::PullBinlog)?;
    info!("successful synchronization");
    Ok(())
}

pub fn push_binlog_info(conf: &Arc<Config>, tcp: &mut TcpStream, buf: &Vec<u8>) -> Result<(), Box<dyn Error>> {
    info!("append difference binlog");
    //info!("{:?}", buf);
    let value: BinlogValue = serde_json::from_slice(&buf[9..])?;
    let reader_size= value.value.len() as u64;
    info!("append {} bytes", reader_size);
    let mut cur = Cursor::new(value.value);
    let mut rowsql = crate::binlog::readbinlog::parse(conf, &mut cur, reader_size, false)?;
    rowsql.set_append_etype();

    let mut conn = crate::create_conn(conf)?;
    for traction in &rowsql.sqls{
        let sqls = &traction.cur_sql;
        for sql in sqls{
            info!("{}",sql);
            crate::io::command::execute_update(&mut conn, sql)?;
        }
    }
    info!("Ok");
    crate::mysql::send_value_packet(&tcp, &rowsql, MyProtocol::RecoveryValue)?;
    Ok(())
}
