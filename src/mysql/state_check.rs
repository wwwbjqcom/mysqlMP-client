/*
@author: xiao cai niao
@datetime: 2019/11/11
*/


use crate::{Config, mysql, readvalue};
use std::sync::{Arc, Mutex};
use std::net::TcpStream;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use chrono::prelude::*;
use chrono;
use std::{thread, time};
use crate::io::socketio;

pub struct LastCheckTime{
    pub last_time: usize,   //最后一次检查的时间
}

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
    pub innodb_buffer_pool_size: usize,
    pub last_sql_error: String,
    pub last_io_error: String,
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
            innodb_buffer_pool_size: 0,
            last_sql_error: "".to_string(),
            last_io_error: "".to_string(),
        }
    }

    pub fn my_clone(&self) -> MysqlState{
        MysqlState{
            online: self.online.clone(),
            role: self.role.clone(),
            master: self.master.clone(),
            sql_thread: self.sql_thread.clone(),
            io_thread: self.io_thread.clone(),
            seconds_behind: self.seconds_behind.clone(),
            master_log_file: self.master_log_file.clone(),
            read_master_log_pos: self.read_master_log_pos.clone(),
            exec_master_log_pos: self.exec_master_log_pos.clone(),
            read_only: self.read_only.clone(),
            version: self.version.clone(),
            executed_gtid_set: self.executed_gtid_set.clone(),
            innodb_flush_log_at_trx_commit: self.innodb_flush_log_at_trx_commit.clone(),
            sync_binlog: self.sync_binlog.clone(),
            server_id: self.server_id.clone(),
            event_scheduler: self.event_scheduler.clone(),
            innodb_buffer_pool_size: self.innodb_buffer_pool_size.clone(),
            last_sql_error: self.last_sql_error.clone(),
            last_io_error: self.last_io_error.clone()
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
        if let Some(io_err) = result.get(&String::from("Last_IO_Error")){
            self.last_io_error = io_err.parse()?;
        }
        if let Some(sql_err) = result.get(&String::from("Last_SQL_Error")){
            self.last_sql_error = sql_err.parse()?;
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
                                            @@event_scheduler,\
                                            @@innodb_buffer_pool_size;");
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
        if let Some(pool_size) = result.get(&String::from("innodb_buffer_pool_size")){
            self.innodb_buffer_pool_size = pool_size.parse()?;
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


pub struct MysqlConn{
    pub conn: TcpStream,
    pub state: Arc<Mutex<MysqlState>>,
    pub laste_check_time: Arc<Mutex<LastCheckTime>>,
    pub conf: Arc<Config>,
    pub conn_state: bool
}

impl MysqlConn{
    pub fn new(state: Arc<Mutex<MysqlState>>, laste_check_time: Arc<Mutex<LastCheckTime>>, conf: Arc<Config>) -> Result<MysqlConn, Box<dyn Error>> {
        let my_conf = conf.clone_new();
        let conn = crate::create_conn(&my_conf)?;
        return Ok(MysqlConn{conn, state, laste_check_time, conf, conn_state: true });
    }


    /// 循环获取mysql各状态数据
    pub fn loop_state_check(&mut self) -> Result<(), Box<dyn Error>> {
        loop {
            if let Err(e) = self.tcp_health_check(){
                self.conn_state = false;
                let a = e.to_string();
                if a.to_lowercase().contains("too many connections"){
                    self.set_value_for_error()?;
                    thread::sleep(time::Duration::from_secs(1));
                    continue;
                }
                if let Err(e) = self.set_state_to_default(){
                    info!("status check thread failed to initialize default data");
                };
            }else {
                self.conn_state = true;
                if let Err(e) = self.check(){
                    info!("set mysql state error: {:?}", e.to_string());
                };
            }
            thread::sleep(time::Duration::from_secs(1));
        }
    }

    /// 发生too many connections报错时， 依然设置mysql检查状态为正常， 防止切换
    fn set_value_for_error(&mut self) -> Result<(), Box<dyn Error>>{
        let mut state_lock = self.state.lock().unwrap();
        state_lock.online = true;
        let mut last_lock = self.laste_check_time.lock().unwrap();
        last_lock.last_time = Local::now().timestamp_millis() as usize;
        Ok(())
    }

    /// 获取数据
    fn check(&mut self) -> Result<(), Box<dyn Error>> {
        let mut state_lock = self.state.lock().unwrap();
        state_lock.slave_state_check(&mut self.conn)?;
        state_lock.variable_check(&mut self.conn)?;
        state_lock.gtid_check(&mut self.conn)?;
        state_lock.online = true;
        let mut last_check_time_lock = self.laste_check_time.lock().unwrap();
        last_check_time_lock.last_time = Local::now().timestamp_millis() as usize;
        Ok(())
    }

    /// 设置mysql状态为false
    fn set_state_to_default(&mut self) -> Result<(), Box<dyn Error>>{
        let mut state_lock = self.state.lock().unwrap();
        state_lock.online = false;
        Ok(())
    }

    /// 发送ping 检查连接可用性
    pub fn tcp_health_check(&mut self) -> Result<(), Box<dyn Error>>{
        if self.conn_state{
            let mut packet: Vec<u8> = vec![];
            packet.extend(readvalue::write_u24(1));
            packet.push(0);
            packet.push(0x0e);
            socketio::write_value(&mut self.conn,&packet)?;
            let get_state = socketio::get_packet_from_stream(&mut self.conn);
            match get_state{
                Ok((buf, _)) => {
                    if buf[0] == 0x00{
                        return Ok(());
                    }else {
                        self.create_my_conn()?;
                    }
                }
                Err(e) => {
                    self.create_my_conn()?;
                }
            }
        }else {
            self.create_my_conn()?;
        }
        return Ok(())
    }

    fn create_my_conn(&mut self) -> Result<(), Box<dyn Error>>{
        for _ in 0..3{
            let tcp_conn = crate::create_conn(&self.conf);
            match tcp_conn {
                Ok(conn) => {
                    self.conn = conn;
                    self.conn_state = true;
                    return Ok(())
                }
                Err(e) => {
                    let a = e.to_string();
                    info!("create connection error: {:?}", &a);
                    if a.to_lowercase().contains("too many connections"){
                        return Err(String::from("too many connections").into());
                    }
                }
            }
            thread::sleep(time::Duration::from_millis(50));
        }
        return Err(String::from("create mysql connection failed").into());
    }
}



pub fn mysql_state_check(tcp: &TcpStream, state: &Arc<Mutex<MysqlState>>, last_check_time: &Arc<Mutex<LastCheckTime>>) -> Result<(), Box<dyn Error>> {
//    let mut state = MysqlState::new();
//    let tcp_conn = crate::create_conn(&conf);
//    match tcp_conn {
//        Ok(mut conn) => {
//            state.online = true;
//            state.slave_state_check(&mut conn)?;
//            state.variable_check(&mut conn)?;
//            state.gtid_check(&mut conn)?;
//            crate::io::command::close(&mut conn);
//        }
//        Err(e) => {
//            info!("{:?}", e.to_string());
//        }
//    }
    let state_lock = state.lock().unwrap();
    let last_lock = last_check_time.lock().unwrap();
    let now_time = Local::now().timestamp_millis() as usize;
    if now_time - last_lock.last_time >= 10000{
        let state = MysqlState::new();
        //info!("the status data lags behind for more than 10s, send mysql default packet");
        mysql::send_value_packet(&tcp, &state, mysql::MyProtocol::MysqlCheck)?;
        return Ok(())
    }
    let my_state = state_lock.my_clone();
    mysql::send_value_packet(&tcp, &my_state, mysql::MyProtocol::MysqlCheck)?;
    Ok(())
}




