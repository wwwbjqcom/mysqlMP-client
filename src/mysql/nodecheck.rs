/*
@author: xiao cai niao
@datetime: 2019/11/22
*/

use std::net::TcpStream;
use std::sync::Arc;
use crate::Config;
use std::error::Error;
use crate::mysql::state_check::MysqlState;
use crate::mysql::{MyProtocol, conn, send_packet, rec_packet};
use serde::{Serialize, Deserialize};

///
/// 分发到client请求检查宕机节点状态
///
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DownNodeCheck {
    pub host: String,
    pub dbport: usize,
}

///
/// client回复检查宕机节点状态数据
///
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DownNodeCheckStatus {
    pub host: String,
    pub client_status: bool,
    pub db_status: bool
}
impl DownNodeCheckStatus {
    fn new(host: String) -> DownNodeCheckStatus {
        DownNodeCheckStatus{
            host,
            client_status: true,
            db_status: true
        }
    }

    fn set_client_status(&mut self) {
        self.client_status = false;
    }
    fn set_db_status(&mut self) {
        self.db_status = false;
    }

}

///
/// 复检宕机节点状态
///     首先通过state_check方式检查
///     再通过直连db检查
///
pub fn check_down_node(tcp: &mut TcpStream,conf: &Arc<Config>) -> Result<(), Box<dyn Error>> {
    let value = crate::io::get_network_packet(tcp)?;
    let value: DownNodeCheck = serde_json::from_slice(&value)?;
    println!("check status: {:?}....",value);
    let mut node_state = DownNodeCheckStatus::new(value.host.clone());
    let state = get_node_state_from_host(&value.host);
    match state {
        Ok(v) => {
            println!("{:?}",v);
            if !v.online {
                node_state.set_db_status();
            }
            crate::mysql::send_value_packet(&tcp, &node_state, MyProtocol::DownNodeCheck)?;
            return Ok(());
        }
        Err(e) => {
            node_state.set_client_status();
        }
    }
    if let Err(_e) = crate::create_conn(conf) {
        node_state.set_db_status();
    }
    println!("{:?}",node_state);
    crate::mysql::send_value_packet(&tcp, &node_state, MyProtocol::DownNodeCheck)?;
    return Ok(());

}

fn get_node_state_from_host(host_info: &str) -> Result<MysqlState, Box<dyn Error>> {
    let mut conn = conn(host_info)?;
    let mut buf: Vec<u8> = vec![];
    buf.push(0xfe);
    send_packet(&buf, &mut conn)?;
    let packet = rec_packet(&mut conn)?;
    let type_code = MyProtocol::new(&packet[0]);
    match type_code {
        MyProtocol::MysqlCheck => {
            let value: MysqlState = serde_json::from_slice(&packet[9..])?;
            return Ok(value);
        }
        _ => {
            let a = format!("return invalid type code: {}",&packet[0]);
            return  Box::new(Err(a)).unwrap();
        }
    }
}