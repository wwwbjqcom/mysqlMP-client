/*
@author: xiao cai niao
@datetime: 2019/11/11
*/
use serde::{Deserialize};
use std::net::TcpStream;
use std::io::{Read, Seek, SeekFrom};
use serde_json;
use std::sync::Arc;
use crate::Config;
use crate::binlog::open_file;
use crate::binlog::readevent::{Tell};
use crate::binlog::readbinlog::{ RowsSql};
use std::error::Error;
use crate::mysql::MyProtocol;

#[derive(Deserialize, Debug)]
pub struct RecoveryInfo {
    binlog: String,
    position: usize,
    gtid: String,
    masterhost: String,
    masterport: usize,
}

///
/// 用于宕机重启时自动恢复主从关系以及保持数据一致性
/// 服务端发送宕机时新master的binlog以及gtid信息，
/// 客户端接受到之后进行判断是否有回滚的数据，然后重新建立主从关系
///
pub fn recovery_my_slave(tcp: &mut TcpStream,conf: &Arc<Config>) -> Result<(), Box<dyn Error>>{
    println!("start");
//    let mut header = [0u8; 8];
//    tcp.read_exact(&mut header)?;
//    let value_length = crate::readvalue::read_u64(&header);
//    let mut value = vec![0u8; value_length as usize];
//    tcp.read_exact(&mut value)?;
    let value = crate::io::get_network_packet(tcp)?;
    let rec_info: RecoveryInfo = serde_json::from_str(crate::readvalue::read_string_value(&value).as_ref())?;
    //println!("{:?}",rec_info);

    let path = format!("{}/{}",conf.binlogdir,rec_info.binlog);
    let mut reader = open_file(&path)?;
    reader.seek(SeekFrom::End(0))?;
    let end_pos = reader.tell()?;
    if end_pos <= rec_info.position as u64 {
        println!("no rollback data ");
        return Ok(());
    }
    println!("need to recover {} bytes of data", end_pos as usize - rec_info.position);
    reader.seek(SeekFrom::Start(rec_info.position as u64))?;
    let result = crate::binlog::readbinlog::readbinlog_fromfile(conf, &mut reader, end_pos);
    match result {
        Ok(r) => {
            recovery_rows(conf , &r, &rec_info)?;
            crate::mysql::send_value_packet(&tcp, &r, MyProtocol::RecoveryValue)?;
        }
        Err(e) => {
            let reponse_err = crate::mysql::ReponseErr::new((*e.to_string()).parse()?);
            crate::mysql::send_value_packet(&tcp, &reponse_err, MyProtocol::Error)?
        }
    }
    Ok(())
}
///
/// 执行回滚语句及change master命令
///
fn recovery_rows(conf: &Arc<Config>, sqls: &RowsSql, rec_info: &RecoveryInfo) -> Result<(), Box<dyn Error>>{
    let conn = crate::create_conn(conf);
    match conn {
        Ok(mut tcp) => {
            for traction in &sqls.sqls{
                let sql = &traction.cur_sql;
                match_state(recovery_traction(&mut tcp, sql))?;
                match_state(change_master(&mut tcp, conf, rec_info))?;
                match_state(crate::mysql::set_readonly(&mut tcp))?;
            }
            return Ok(())
        }
        Err(e) => {
            let a: Box<dyn Error> = e.into();
            return Err(a);
        }
    }
}

fn recovery_traction(tcp: &mut TcpStream, sqls: &Vec<String>) -> Result<(), Box<dyn Error>> {
    for sql in sqls{
        crate::io::command::execute_update(tcp, &sql)?;
    }
    Ok(())
}

///
/// 根据服务端发送的gtid信息进行change master修改，并启动主从复制
///
fn change_master(tcp: &mut TcpStream, conf: &Arc<Config>, rec_info: &RecoveryInfo) -> Result<(), Box<dyn Error>> {
    crate::io::command::execute_update(tcp, &String::from("reset master;"))?;
    let sql = format!("set global gtid_purged = '{}'", rec_info.gtid);
    crate::io::command::execute_update(tcp, &sql)?;
    let sql = format!("change master to master_host='{}',\
                                master_port='{}',master_user='{}',\
                                master_password='{}',\
                                master_auto_position=1 for channel 'default'",
                      rec_info.masterhost,rec_info.masterport,conf.repl_user,conf.repl_passwd);
    crate::io::command::execute_update(tcp, &sql)?;
    crate::io::command::execute_update(tcp, &String::from("start slave"))?;
    Ok(())
}

fn match_state(sate: Result<(), Box<dyn Error>>) -> Result<(), Box<dyn Error>> {
    match sate {
        Ok(_t) => Ok(()),
        Err(e) => Err(e)
    }
}
