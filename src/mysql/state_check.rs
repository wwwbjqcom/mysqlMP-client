/*
@author: xiao cai niao
@datetime: 2019/11/11
*/


use crate::Config;
use std::sync::{Arc};
use std::net::TcpStream;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;

#[derive(Serialize, Deserialize, Debug)]
pub struct MysqlState {
    pub online: bool,
    pub role: String,
    pub sql_thread: bool,
    pub io_thread: bool,
    pub seconds_behind: usize,
    pub master_log_file: String,
    pub read_master_log_pos: usize,
    pub exec_master_log_pos: usize,
    pub error: String
}

impl MysqlState {
    pub fn new() -> MysqlState {
        MysqlState{
            online: false,
            role: "".to_string(),
            sql_thread: false,
            io_thread: false,
            seconds_behind: 0,
            master_log_file: "".to_string(),
            read_master_log_pos: 0,
            exec_master_log_pos: 0,
            error: "".to_string()
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

        Ok(())
    }
}

pub fn mysql_state_check(conf: Arc<Config>) -> MysqlState {
    let mut state = MysqlState::new();
    let conn = crate::create_conn(&conf);
    match conn {
        Ok(mut tcp) => {
            state.online = true;
            slave_state_check(&mut tcp, &mut state);
        }
        Err(e) => {
            info!("{:?}",e);
            state.online = false;
        }
    }
    return state;
    //sleep 1 seconds
//    use std::{thread, time};
//    let ten_millis = time::Duration::from_secs(1);
//    thread::sleep(ten_millis);

}

fn slave_state_check(tcp: &mut TcpStream, state: &mut MysqlState) {
    let sql = "show slave status;";
    let result= crate::io::command::execute(tcp, &sql.to_string());
    match result {
        Ok(result) => {
            //info!("{:?}", result);
            if result.len() > 0 {
                if let Err(e) = state.update(&result[0]){
                    info!("{:?}", e.to_string());
                };
            }else {
                state.role = String::from("master");
            }
        }
        Err(e) => {
            state.error = (*e.to_string()).parse().unwrap();
        }
    }

}


