/*
@author: xiao cai niao
@datetime: 2019/9/21
*/
use std::net::TcpStream;
use std::error::Error;
use std::io::Read;

pub mod connection;
pub mod socketio;
pub mod response;
pub mod pack;
pub mod scramble;
pub mod command;

pub fn get_network_packet(tcp: &mut TcpStream) -> Result<Vec<u8>,Box<dyn Error>> {
    let mut header = [0u8; 8];
    tcp.read_exact(&mut header)?;
    let value_length = crate::readvalue::read_u64(&header);
    let mut value = vec![0u8; value_length as usize];
    tcp.read_exact(&mut value)?;
    Ok(value)
}