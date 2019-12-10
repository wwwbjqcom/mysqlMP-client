/*
@author: xiao cai niao
@datetime: 2019/11/11
*/
use serde::Deserialize;
use std::net::TcpStream;
use std::sync::Arc;
use crate::{Config};
use std::error::Error;
use crate::mysql::{ReponseErr};

#[derive(Deserialize)]
pub struct ChangeMasterInfo{
    pub master_host: String,
    pub master_port: usize,
}

pub fn change_master(mut tcp: &TcpStream, conf: &Arc<Config>, buf: &Vec<u8>) -> Result<(), Box<dyn Error>> {
    let conn = crate::create_conn(conf);
    match conn {
        Ok(mut db_tcp) => {
            //let value = crate::io::get_network_packet(&mut tcp)?;
            let value = &buf[9..];
            let change_info: ChangeMasterInfo = serde_json::from_str(crate::readvalue::read_string_value(value).as_ref())?;
            info!("change master to {}", &change_info.master_host);
            if let Err(e) = change_master_info(&mut db_tcp, conf, &change_info){
                let err = e.to_string();
                info!("Error: {}", &err);
                crate::mysql::send_error_packet(&ReponseErr::new(err), &mut tcp)?;
                return Ok(());
            };
            crate::mysql::send_ok_packet(tcp)?;
            info!("change Ok!");
            return Ok(());
        }
        Err(e) => {
            let err = e.to_string();
            info!("{:?}", &err);
            crate::mysql::send_error_packet(&ReponseErr::new(err), &mut tcp)?;
            return Ok(());
        }
    }
}

fn change_master_info(tcp: &mut TcpStream, conf: &Arc<Config>, change_info: &ChangeMasterInfo) -> Result<(), Box<dyn Error>>{
    let stop_slave = String::from("stop slave for channel 'default';");
    let reset_slave_sql = String::from("reset slave for channel 'default';");
    let change_sql = format!("change master to master_host='{}',\
                                master_port={},master_user='{}',\
                                master_password='{}',\
                                master_auto_position=1 for channel 'default'",
                             change_info.master_host,change_info.master_port,conf.repl_user,conf.repl_passwd);

    info!("{}", &stop_slave);
    if let Err(s) = crate::io::command::execute_update(tcp, &stop_slave){
        info!("{}",s.to_string());
    };
    info!("{}", &reset_slave_sql);
    if let Err(e) = crate::io::command::execute_update(tcp, &reset_slave_sql){
        info!("{}",e.to_string());
    };
    info!("{}", &change_sql);
    crate::io::command::execute_update(tcp, &change_sql)?;
    info!("start slave");
    crate::io::command::execute_update(tcp, &String::from("start slave"))?;
    info!("set readonly");
    crate::mysql::set_readonly(tcp)?;
    Ok(())
}

pub fn set_variabels(tcp: &mut TcpStream, conf: &Arc<Config>) -> Result<(), Box<dyn Error>> {
    let mut conn = crate::create_conn(conf)?;
    crate::mysql::set_readonly(&mut conn)?;
    crate::mysql::send_ok_packet(tcp)?;
    Ok(())
}

