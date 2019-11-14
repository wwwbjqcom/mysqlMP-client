/*
@author: xiao cai niao
@datetime: 2019/11/11
*/


use crate::Config;
use std::sync::{Arc};
use std::net::TcpStream;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct MysqlState {
    pub online: bool,
    pub role: String,
    pub sql_thread: bool,
    pub io_thread: bool,
    pub seconds_behind: usize,
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
            error: "".to_string()
        }
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
            println!("{:?}",e);
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
            if result.len() > 0 {
                for row in result{
                    state.role = String::from("slave");
                    let slave_io_state = row.get(&String::from("Slave_IO_Running")).unwrap();
                    if slave_io_state == "Yes"{
                        state.io_thread = true;
                    }else {
                        state.io_thread = false;
                    }

                    let sql_thread_state = row.get(&String::from("Slave_SQL_Running")).unwrap();
                    if sql_thread_state == "Yes"{
                        state.sql_thread = true;
                    }else {
                        state.sql_thread = false;
                    }

                    state.seconds_behind = row.get(&String::from("Seconds_Behind_Master")).unwrap().parse().unwrap();
                }

            }else {
                state.role = String::from("master");
            }
        }
        Err(e) => {
            state.error = (*e.to_string()).parse().unwrap();
        }
    }

}


