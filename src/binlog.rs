/*
@author: xiao cai niao
@datetime: 2019/11/12
*/

pub mod readbinlog;
pub mod readevent;
pub mod parsevalue;
pub mod jsonb;
pub mod rollback;
pub mod getsql;
use std::fs::File;
use std::io::{BufReader};

//pub struct BinlogInfo {
//    pub all_traction: Vec<Vec<String>>,
//    pub rollback_traction:  Vec<Vec<String>>
//}
//
//impl BinlogInfo {
//    pub fn new(info: &RecoveryInfo, conf: &Arc<Config>) -> Result<BinlogInfo, &'static str> {
//        println!("start rolling back difference data");
//        let mut path = &conf.binlogdir;
//        let mut reader = open_file(&path)?;
//        let end_pos = reader.seek(SeekFrom::End(0))?;
////        let a = info;
//
////        if end_pos <=
////        crate::binlog::readbinlog::readbinlog_fromfile(info, &version, &mut reader);
////        Ok(BinlogInfo{ all_traction: vec![], rollback_traction: vec![] })
//    }
//}



pub fn open_file(path: &String) -> Result<BufReader<File>,std::io::Error> {
    let f = File::open(path)?;
    let reader = BufReader::new(f);
    return Ok(reader);
}



