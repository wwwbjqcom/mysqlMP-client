/*
@author: xiao cai niao
@datetime: 2019/11/11
*/

use std::net::{TcpStream, IpAddr, Ipv4Addr, SocketAddr};
use std::io::{Write, Read};

pub mod state_check;
pub mod monitor;
pub mod slowlog;
pub mod audit;
pub mod syncbinlog;
pub mod setmaster;
pub mod changemaster;
pub mod recovery;
pub mod nodecheck;
use serde::{Serialize};
use std::error::Error;
use std::time::Duration;

#[derive(Debug, Serialize)]
pub enum  MyProtocol {
    MysqlCheck,
    GetMonitor,
    GetSlowLog,
    GetAuditLog,
    SetMaster,          //设置本机为新master
    ChangeMaster,
    SyncBinlog,         //mysql服务宕机，同步差异binlog到新master
    RecoveryCluster,    //宕机重启自动恢复主从同步, 如有差异将回滚本机数据，并保存回滚数据
    RecoveryValue,      //宕机恢复回滚的数据，回给服务端保存，有管理员人工决定是否追加
    ReplicationStatus,  //获取slave同步状态
    DownNodeCheck,      //宕机节点状态检查，用于server端检测到宕机时，分发到各client复检
    Ok,
    Error,
    UnKnow
}
impl MyProtocol {
    pub fn new(code: &u8) -> MyProtocol{
        if code == &0xfe {
            return MyProtocol::MysqlCheck;
        }else if code == &0xfd {
            return MyProtocol::GetMonitor;
        }else if code == &0xfc {
            return MyProtocol::GetSlowLog;
        }else if code == &0xfb {
            return MyProtocol::GetAuditLog;
        }else if code == &0xfa {
            return MyProtocol::SetMaster;
        }else if code == &0xf9 {
            return MyProtocol::ChangeMaster;
        }else if code == &0xf8 {
            return MyProtocol::SyncBinlog;
        }else if code == &0xf7 {
            return MyProtocol::RecoveryCluster;
        }else if code == &0x00 {
            return MyProtocol::Ok;
        }else if code == &0x09 {
            return MyProtocol::Error;
        }else if code == &0xf6 {
            return MyProtocol::RecoveryValue;
        }else if code == &0xf5 {
            return MyProtocol::ReplicationStatus;
        }else if code == &0xf4 {
            return MyProtocol::DownNodeCheck;
        }
        else {
            return MyProtocol::UnKnow;
        }
    }

    pub fn get_code(&self) -> u8 {
        match self {
            MyProtocol::MysqlCheck => 0xfe,
            MyProtocol::GetMonitor => 0xfd,
            MyProtocol::GetSlowLog => 0xfc,
            MyProtocol::GetAuditLog => 0xfb,
            MyProtocol::SetMaster => 0xfa,
            MyProtocol::ChangeMaster => 0xf9,
            MyProtocol::SyncBinlog => 0xf8,
            MyProtocol::RecoveryCluster => 0xf7,
            MyProtocol::Ok => 0x00,
            MyProtocol::Error => 0x09,
            MyProtocol::RecoveryValue => 0xf6,
            MyProtocol::ReplicationStatus => 0xf5,
            MyProtocol::DownNodeCheck => 0xf4,
            MyProtocol::UnKnow => 0xff
        }
    }

}

#[derive(Serialize)]
pub struct ReponseErr{
    pub err: String
}

impl ReponseErr {
    pub fn new(value: String) -> ReponseErr {
        ReponseErr{
            err: value
        }
    }
}

pub fn send_error_packet(value: &ReponseErr, mut tcp: &TcpStream) -> Result<(), std::io::Error> {
    let value = serde_json::to_string(value)?;
    let mut buf = header(0x09, value.len() as u64);
    buf.extend(value.as_bytes());
    tcp.write(buf.as_ref())?;
    tcp.flush()?;
    Ok(())
}

pub fn write_value<T: Serialize>(mut tcp: &TcpStream, value: &T) -> Result<(), std::io::Error> {
    let mut buf: Vec<u8> = vec![];
    let value = serde_json::to_string(value).unwrap();
    buf.extend(crate::readvalue::write_u64(value.len() as u64));
    buf.extend(value.as_bytes());
    tcp.write(buf.as_ref())?;
    tcp.flush()?;
    Ok(())
}

pub fn send_ok_packet(mut tcp: &TcpStream) -> Result<(), std::io::Error> {
    let mut buf: Vec<u8> = vec![];
    buf.push(0x09);
    tcp.write(buf.as_ref())?;
    tcp.flush()?;
    Ok(())
}

pub fn send_value_packet<T: Serialize>(mut tcp: &TcpStream, value: &T, type_code: MyProtocol) -> Result<(), Box<dyn Error>> {
    let value = serde_json::to_string(value)?;
    let mut buf = header(type_code.get_code(), value.len() as u64);
    buf.extend(value.as_bytes());
    tcp.write(buf.as_ref())?;
    tcp.flush()?;
    Ok(())
}

pub fn send_packet(packet: &Vec<u8>, conn: &mut TcpStream) -> Result<(), Box<dyn Error>>{
    conn.write(packet)?;
    conn.flush()?;
    Ok(())
}

///
/// 接收client返回的数据
///
pub fn rec_packet(conn: &mut TcpStream) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut buf: Vec<u8> = vec![];
    let mut header: Vec<u8> = vec![0u8;9];
    conn.read_exact(&mut header)?;
    let payload = crate::readvalue::read_u64(&header[1..]);
    let mut payload_buf: Vec<u8> = vec![0u8; payload as usize];
    conn.read_exact(&mut payload_buf)?;
    buf.extend(header);
    buf.extend(payload_buf);
    Ok(buf)
}

fn header(code: u8, payload: u64) -> Vec<u8> {
    let mut buf: Vec<u8> = vec![];
    buf.push(code);
    let payload = crate::readvalue::write_u64(payload);
    buf.extend(payload);
    return buf;
}



pub fn set_readonly(tcp: &mut TcpStream) -> Result<(), Box<dyn Error>> {
    let set_read_only = String::from("set global read_only=1;");
    let set_sync_binlog = String::from("set global sync_binlog=0;");
    let set_flush_redo = String::from("set global innodb_flush_log_at_trx_commit=0;");
    crate::io::command::execute_update(tcp, &set_read_only)?;
    crate::io::command::execute_update(tcp, &set_sync_binlog)?;
    crate::io::command::execute_update(tcp, &set_flush_redo)?;
    Ok(())
}

pub fn set_no_readonly(tcp: &mut TcpStream) -> Result<(), Box<dyn Error>> {
    let set_read_only = String::from("set global read_only=0;");
    let set_sync_binlog = String::from("set global sync_binlog=1;");
    let set_flush_redo = String::from("set global innodb_flush_log_at_trx_commit=1;");
    crate::io::command::execute_update(tcp, &set_read_only)?;
    crate::io::command::execute_update(tcp, &set_sync_binlog)?;
    crate::io::command::execute_update(tcp, &set_flush_redo)?;
    Ok(())
}


pub fn check_state(state: &Result<(), Box<dyn Error>>) {
    match state {
        Ok(()) => {}
        Err(e) => {
            println!("{:?}",e);
        }
    }
}


fn conn(host_info: &str) -> Result<TcpStream, Box<dyn Error>> {
    let host_info = host_info.split(":");
    let host_vec = host_info.collect::<Vec<&str>>();
    let port = host_vec[1].to_string().parse::<u16>()?;
    let ip_vec = host_vec[0].split(".");
    let ip_vec = ip_vec.collect::<Vec<&str>>();
    let mut ip_info = vec![];
    for i in ip_vec{
        ip_info.push(i.to_string().parse::<u8>()?);
    }
    let addrs = SocketAddr::from((IpAddr::V4(Ipv4Addr::new(ip_info[0], ip_info[1], ip_info[2], ip_info[3])), port));
    //let tcp_conn = TcpStream::connect(host_info)?;
    let tcp_conn = TcpStream::connect_timeout(&addrs, Duration::new(2,5))?;
    tcp_conn.set_read_timeout(Some(Duration::new(10,10)))?;
    tcp_conn.set_write_timeout(Some(Duration::new(10,10)))?;
    Ok(tcp_conn)
}




