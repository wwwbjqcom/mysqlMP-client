/*
@author: xiao cai niao
@datetime: 2019/11/11
*/


use crate::{Config, mysql};
use std::sync::{Arc};
use std::net::TcpStream;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;

#[derive(Serialize, Deserialize, Debug)]
pub struct MysqlState {
    pub online: bool,
    pub role: String,
    pub master: String,
    pub sql_thread: bool,
    pub io_thread: bool,
    pub seconds_behind: usize,
    pub master_log_file: String,
    pub read_master_log_pos: usize,
    pub exec_master_log_pos: usize,
    pub read_only: bool,
    pub version: String,
    pub executed_gtid_set: String,
    pub innodb_flush_log_at_trx_commit: usize,
    pub sync_binlog: usize,
    pub server_id: usize,
    pub event_scheduler: String,
    pub sql_error: String
}

impl MysqlState {
    pub fn new() -> MysqlState {
        MysqlState{
            online: false,
            role: "".to_string(),
            master: "".to_string(),
            sql_thread: false,
            io_thread: false,
            seconds_behind: 0,
            master_log_file: "".to_string(),
            read_master_log_pos: 0,
            exec_master_log_pos: 0,
            read_only: false,
            version: "".to_string(),
            executed_gtid_set: "".to_string(),
            innodb_flush_log_at_trx_commit: 0,
            sync_binlog: 0,
            server_id: 0,
            event_scheduler: "OFF".to_string(),
            sql_error: "".to_string()
        }
    }

    pub fn update(&mut self, result: &HashMap<String, String>) -> Result<(), Box<dyn Error>> {
        self.role = String::from("slave");

        if let Some(slave_io_state) = result.get(&String::from("Slave_IO_Running")){
            if slave_io_state == "Yes"{
                self.io_thread = true;
            }else {
                self.io_thread = false;
            }
        };

        if let Some(sql_thread_state)= result.get(&String::from("Slave_SQL_Running")){
            if sql_thread_state == "Yes"{
                self.sql_thread = true;
            }else {
                self.sql_thread = false;
            }
        };

        if let Some(behind) = result.get(&String::from("Seconds_Behind_Master")){
            //info!("{},{:?}", behind, Some(behind));
            if behind.len() > 0 {
                self.seconds_behind = behind.parse()?;
            }
        }

        if let Some(log_file) = result.get(&String::from("Master_Log_File")){
            self.master_log_file = log_file.parse()?;
        }

        if let Some(read_pos) = result.get(&String::from("Read_Master_Log_Pos")){
            self.read_master_log_pos = read_pos.parse()?;
        }

        if let Some(exec_pos) = result.get(&String::from("Exec_Master_Log_Pos")){
            self.exec_master_log_pos = exec_pos.parse()?;
        }
        if let Some(sql_err) = result.get(&String::from("Slave_SQL_Running_State")){
            self.sql_error = sql_err.parse()?;
        }
        if let Some(master_host) = result.get(&String::from("Master_Host")){
            self.master = master_host.parse()?;
        }
        Ok(())
    }

    pub fn slave_state_check(&mut self, tcp: &mut TcpStream) -> Result<(), Box<dyn Error>> {
        let sql = "show slave status;";
        let result= crate::io::command::execute(tcp, &sql.to_string())?;
        if result.len() > 0 {
            mysql::check_state(&self.update(&result[0]));
        }else {
            self.role = String::from("master");
        }
        Ok(())
    }

    pub fn variable_check(&mut self, tcp: &mut TcpStream) -> Result<(), Box<dyn Error>> {
        let sql = String::from("select @@read_only as read_only,\
                                            @@sync_binlog as sync_binlog,\
                                            @@innodb_flush_log_at_trx_commit as innodb_flush_log_at_trx_commit,\
                                            @@version as version,\
                                            @@server_id as server_id,\
                                            @@event_scheduler;");
        let result= crate::io::command::execute(tcp, &sql)?;
        if result.len() > 0 {
            mysql::check_state(&self.update_variable(&result[0]));
        }
        Ok(())
    }

    pub fn update_variable(&mut self, result: &HashMap<String, String>) -> Result<(), Box<dyn Error>> {
        if let Some(v) = result.get(&String::from("read_only")){
            let a: u8 = v.parse()?;
            if a == 1{
             self.read_only = true;
            }else {
             self.read_only = false;
            }
        }
        if let Some(v) = result.get(&String::from("sync_binlog")){
            self.sync_binlog = v.parse()?;
        }

        if let Some(v) = result.get(&String::from("innodb_flush_log_at_trx_commit")){
            self.innodb_flush_log_at_trx_commit = v.parse()?;
        }

        if let Some(v) = result.get(&String::from("version")){
            let version = v.split("-");
            let vv = version.collect::<Vec<&str>>();
            self.version = vv[0].to_string();
        }
        if let Some(v) = result.get(&String::from("server_id")){
            self.server_id = v.parse()?;
        }
        if let Some(v) = result.get(&String::from("event_scheduler")){
            self.event_scheduler = v.parse()?;
        }
        Ok(())
    }

    pub fn gtid_check(&mut self, tcp: &mut TcpStream) -> Result<(), Box<dyn Error>> {
        let sql = String::from("show master status");
        let result= crate::io::command::execute(tcp, &sql)?;
        if result.len() > 0 {
            if let Some(v) = result[0].get(&String::from("Executed_Gtid_Set")){
                let v: String = v.parse()?;
                self.executed_gtid_set = v.replace("\n","");
            }
        }
        Ok(())
    }
}

pub fn mysql_state_check(tcp: &TcpStream, conf: Arc<Config>) -> Result<(), Box<dyn Error>> {
    let mut state = MysqlState::new();
    let mut conn = crate::create_conn(&conf)?;
    state.online = true;
    state.slave_state_check(&mut conn)?;
    state.variable_check(&mut conn)?;
    state.gtid_check(&mut conn)?;
    mysql::send_value_packet(&tcp, &state, mysql::MyProtocol::MysqlCheck)?;
    Ok(())
}




