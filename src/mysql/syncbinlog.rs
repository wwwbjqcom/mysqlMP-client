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
use std::io::{Seek, SeekFrom, BufReader, Read};
use crate::binlog::readevent::Tell;
use std::fs::File;
use crate::mysql::MyProtocol;

#[derive(Deserialize)]
pub struct SyncBinlogInfo{
    binlog: String,
    position: usize
}

#[derive(Serialize)]
pub struct BinlogValue{
    value: Vec<u8>
}
impl BinlogValue{
    fn new(value: Vec<u8>) -> BinlogValue {
        BinlogValue{value}
    }
}

pub fn sync_binlog_info(conf: &Arc<Config>, tcp: &mut TcpStream) -> Result<(), Box<dyn Error>>{
    println!("ynchronization difference binlog");
    let value = crate::io::get_network_packet(tcp)?;
    let sync_info: SyncBinlogInfo = serde_json::from_str(crate::readvalue::read_string_value(&value).as_ref())?;
    println!("start position {}",sync_info.position);
    let path = format!("{}/{}",conf.binlogdir,sync_info.binlog);
    let mut reader = open_file(&path)?;
    reader.seek(SeekFrom::End(0))?;
    let end_pos = reader.tell()?;
    let read_bytes = end_pos - sync_info.position as u64;
    if read_bytes > 0{
        let mut value: Vec<u8> = vec![];
        reader.seek(SeekFrom::Start(sync_info.position as u64))?;
        reader.read_to_end(&mut value)?;
        let value = BinlogValue::new(value);
        crate::mysql::send_value_packet(tcp, &value, MyProtocol::SyncBinlog)?;
    }
    println!("successful synchronization");
    Ok(())
}
