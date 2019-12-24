/*
@author: xiao cai niao
@datetime: 2019/11/11
*/
use serde::{Deserialize, Serialize};
use std::net::TcpStream;
use std::io::{Seek, SeekFrom};
use serde_json;
use std::sync::Arc;
use crate::Config;
use crate::binlog::open_file;
use crate::binlog::readevent::{Tell};
use crate::binlog::readbinlog::{ RowsSql};
use std::error::Error;
use crate::mysql::{MyProtocol, Null};

#[derive(Deserialize, Debug)]
pub struct RecoveryInfo {
    binlog: String,
    position: usize,
    gtid: String,
    masterhost: String,
    masterport: usize,
    read_binlog: String,
    read_position: usize,
}

impl RecoveryInfo {
    ///
    /// 无需回滚数据，直接恢复同步
    ///
    fn recovery_replication(&self, conf: &Arc<Config>) -> Result<(), Box<dyn Error>> {
        let mut conn = crate::create_conn(conf)?;
        self.change_master(&mut conn, conf)?;
        Ok(())
    }
    ///
    /// 执行回滚语句及change master命令
    ///
    fn recovery_rows(&self, conf: &Arc<Config>, sqls: &RowsSql) -> Result<(), Box<dyn Error>>{
        let conn = crate::create_conn(conf);
        match conn {
            Ok(mut tcp) => {
                let traction_len = sqls.sqls.len();
                for indx in 0..traction_len {
                    let sql = &sqls.sqls[traction_len - indx - 1].rollback_sql;
                    match_state(self.recovery_traction(&mut tcp, sql))?;
                }
                match_state(self.change_master(&mut tcp, conf))?;
                match_state(crate::mysql::set_readonly(&mut tcp))?;
                return Ok(())
            }
            Err(e) => {
                let a: Box<dyn Error> = e.into();
                return Err(a);
            }
        }
    }
    ///
    /// 根据服务端发送的gtid信息进行change master修改，并启动主从复制
    ///
    fn change_master(&self, tcp: &mut TcpStream, conf: &Arc<Config>) -> Result<(), Box<dyn Error>> {
        let reset_master = String::from("reset master;");
        let set_sql = format!("set global gtid_purged = '{}'", self.gtid);
        let change_sql = format!("change master to master_host='{}',\
                                master_port={},master_user='{}',\
                                master_password='{}',\
                                master_auto_position=1 for channel 'default'",
                          self.masterhost,self.masterport,conf.repl_user,conf.repl_passwd);
        info!("{}",&reset_master);
        crate::io::command::execute_update(tcp, &reset_master)?;
        info!("{}",&set_sql);
        crate::io::command::execute_update(tcp, &set_sql)?;
        info!("{}",&change_sql);
        crate::io::command::execute_update(tcp, &change_sql)?;
        info!("start slave");
        crate::io::command::execute_update(tcp, &String::from("start slave"))?;
        Ok(())
    }

    fn recovery_traction(&self, tcp: &mut TcpStream, sqls: &Vec<String>) -> Result<(), Box<dyn Error>> {
        for sql in sqls{
            crate::io::command::execute_update(tcp, &sql)?;
        }
        Ok(())
    }
}



///
/// 用于宕机重启时自动恢复主从关系以及保持数据一致性
/// 服务端发送宕机时新master的binlog以及gtid信息，
/// 客户端接受到之后进行判断是否有回滚的数据，然后重新建立主从关系
///
pub fn recovery_my_slave(tcp: &mut TcpStream, conf: &Arc<Config>, buf: &Vec<u8>) -> Result<(), Box<dyn Error>>{
    info!("start");
    let value = &buf[9..];
    let rec_info: RecoveryInfo = serde_json::from_str(crate::readvalue::read_string_value(value).as_ref())?;
    info!("rec_info: {:?}",rec_info);
    let mut recovery_row = RowsSql::new();
    if rec_info.read_binlog.len() > 0 {
        let path = format!("{}/{}",conf.binlogdir,rec_info.read_binlog);
        info!("config dir: {}", &path);
        let mut reader = open_file(&path)?;
        reader.seek(SeekFrom::End(0))?;
        let end_pos = reader.tell()?;
        if end_pos <= rec_info.read_position as u64 {
            info!("no rollback data ");
            rec_info.recovery_replication(conf)?;
            crate::mysql::send_value_packet(tcp, &Null::new(), MyProtocol::Ok)?;
            return Ok(());
        }
        info!("need to recover {} bytes of data", end_pos as usize - rec_info.read_position);
        reader.seek(SeekFrom::Start(rec_info.read_position as u64))?;
        let result = crate::binlog::readbinlog::parse(conf, &mut reader, end_pos, true);
        match result {
            Ok(r) => {
                info!("{:?}", r);
                recovery_row = r.clone();
                rec_info.recovery_rows(conf , &r)?;
            }
            Err(e) => {
                info!("{}", &e.to_string());
                let reponse_err = crate::mysql::ReponseErr::new((*e.to_string()).parse()?);
                crate::mysql::send_value_packet(&tcp, &reponse_err, MyProtocol::Error)?
            }
        }
        recovery_row.set_rollback_etype();
        crate::mysql::send_value_packet(&tcp, &recovery_row, MyProtocol::RecoveryValue)?;
        return Ok(());
    }else {
        rec_info.recovery_replication(conf)?;
        crate::mysql::send_value_packet(&tcp, &recovery_row, MyProtocol::RecoveryValue)?;
        return Ok(());
    }

}

fn match_state(sate: Result<(), Box<dyn Error>>) -> Result<(), Box<dyn Error>> {
    match sate {
        Ok(_t) => Ok(()),
        Err(e) => Err(e)
    }
}


///
/// 提升为master时服务端获取当前binlog信息
///
#[derive(Deserialize, Debug, Serialize)]
pub struct GetRecoveryInfo {
    pub binlog: String,
    pub position: usize,
    pub gtid: String,
}


impl GetRecoveryInfo {
    pub fn new() -> GetRecoveryInfo {
        GetRecoveryInfo{
            binlog: "".to_string(),
            position: 0,
            gtid: "".to_string()
        }
    }
    pub fn get_state(&mut self, conf: &Arc<Config>) -> Result<(), Box<dyn Error>> {
        let mut conn = crate::create_conn(conf)?;
        let sql = String::from("show master status");
        let result = crate::io::command::execute(&mut conn, &sql)?;

        if result.len() > 0 {
            for row in result {
                if let Some(v) = row.get(&String::from("File")){
                    self.binlog = v.parse()?;
                }
                if let Some(v) = row.get(&String::from("Position")){
                    self.position = v.parse()?;
                }
                if let Some(v) = row.get(&String::from("Executed_Gtid_Set")){
                    self.gtid = v.parse()?;
                }
            }
        }
        Ok(())
    }

}
