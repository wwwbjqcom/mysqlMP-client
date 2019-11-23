/*
@author: xiao cai niao
@datetime: 2019/11/11
*/
use serde::Deserialize;
use std::net::TcpStream;
use std::sync::Arc;
use crate::{Config, mysql};
use std::error::Error;

#[derive(Deserialize)]
pub struct ChangeMasterInfo{
    pub master_host: String,
    pub master_port: String
}

pub fn change_master(mut tcp: &TcpStream, conf: &Arc<Config>) -> Result<(), Box<dyn Error>> {
    let conn = crate::create_conn(conf);
    match conn {
        Ok(mut tcp) => {
            let value = crate::io::get_network_packet(&mut tcp)?;
            let change_info: ChangeMasterInfo = serde_json::from_str(crate::readvalue::read_string_value(&value).as_ref())?;
            let state = change_master_info(&mut tcp, conf, &change_info);
            mysql::check_state(&state);

        }
        Err(e) => {
            println!("{:?}",e);
            let err = mysql::ReponseErr::new(e.parse().unwrap());
            let state = crate::mysql::send_value_packet(&mut tcp, &err, mysql::MyProtocol::Error);
            mysql::check_state(&state);
        }
    }
    crate::mysql::send_ok_packet(tcp);
    Ok(())
}

fn change_master_info(tcp: &mut TcpStream, conf: &Arc<Config>, change_info: &ChangeMasterInfo) -> Result<(), Box<dyn Error>>{
    let stop_slave = String::from("stop slave for channel 'default';");
    let reset_slave_sql = String::from("reset slave for channel 'default';");
    let change_sql = format!("change master to master_host='{}',\
                                master_port='{}',master_user='{}',\
                                master_password='{}',\
                                master_auto_position=1 for channel 'default'",
                             change_info.master_host,change_info.master_port,conf.repl_user,conf.repl_passwd);

    crate::io::command::execute_update(tcp, &stop_slave)?;
    crate::io::command::execute_update(tcp, &reset_slave_sql)?;
    crate::io::command::execute_update(tcp, &change_sql)?;
    crate::io::command::execute_update(tcp, &String::from("start slave"))?;
    Ok(())
}

