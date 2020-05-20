/*
@author: xiao cai niao
@datetime: 2019/9/21
*/

use crate::{Config, readvalue};
use std::net::TcpStream;
use std::process;
use crate::meta;
use crate::io::socketio;
use crate::io::pack;
use crate::io::response;
use std::error::Error;
use std::borrow::Borrow;
use std::io::{Cursor, Read, Seek};
use byteorder::ReadBytesExt;
use crate::io::response::pack_header;
use crate::io::socketio::write_value;
use mysql_common::crypto::encrypt;
use byteorder::LittleEndian;

#[derive(Debug)]
pub enum MysqlPacketType {
    Ok,
    Error,
    Handshake,
    EOF,
    Unknown
}

///
/// 记录每个mysql连接信息
#[derive(Debug)]
pub struct MysqlConnection {
    pub conn: TcpStream,
    pub packet_type: MysqlPacketType,
    pub status: bool,
    pub errorcode: usize,
    pub error: String
}

impl MysqlConnection{
    pub fn new(conf: &Config) -> Result<MysqlConnection, Box<dyn Error>> {
        let conn = crate::mysql::conn(&conf.host_info)?;
        Ok(MysqlConnection{
            conn,
            packet_type: MysqlPacketType::Unknown,
            status: false,
            errorcode: 0,
            error: "".to_string()
        })
    }

    pub fn create(&mut self, conf: &Config) -> Result<(), Box<dyn Error>>{
        let (packet_buf,_) = socketio::get_packet_from_stream(&mut self.conn);
        let mut read_buf = Cursor::new(packet_buf);
        self.get_packet_type(&mut read_buf)?;
        match self.packet_type{
            MysqlPacketType::Handshake => {
                let mut packet_buf = vec![];
                read_buf.read_to_end(&mut packet_buf)?;
                let handshake = pack::HandshakePacket::new(&packet_buf)?;
                self.create_conn(conf, &handshake)?;
            }
            MysqlPacketType::Error => {
                self.errorcode = read_buf.read_u16::<LittleEndian>()?.into();
                let mut a = vec![];
                read_buf.read_to_end(&mut a)?;
                self.error = readvalue::read_string_value(&a)
            }
            _ => {}
        }
        Ok(())
    }

    pub fn create_conn(&mut self, conf: &Config, handshake: &pack::HandshakePacket) -> Result<(), Box<dyn Error>>{
        //根据服务端发送的hand_shake包组回报并发送
        let handshake_response = response::LocalInfo::new(conf.program_name.borrow(), conf.database.len() as u8);
        let packet_type = meta::PackType::HandShakeResponse;
        let v = response::LocalInfo::pack_payload(
            &handshake_response,&handshake,&packet_type,conf)?;

        socketio::write_value(&mut self.conn, v.as_ref())?;

        //检查服务端回包情况
        let (packet_buf,_) = socketio::get_packet_from_stream(&mut self.conn);


        let mut tmp_auth_data = vec![];
        if packet_buf[0] == 0xFE {
            //重新验证密码
            let (auth_data, tmp) = response::authswitchrequest(&handshake, packet_buf.as_ref(), conf);
            tmp_auth_data = tmp;
            socketio::write_value(&mut self.conn, auth_data.as_ref())?;
        }

        let (packet_buf,_) = socketio::get_packet_from_stream(&mut self.conn);
        if pack::check_pack(&packet_buf) {
            if packet_buf[1] == 4{
                if self.sha2_auth( &tmp_auth_data, conf)?{
                    self.status = true;
                }
            }else if packet_buf[1] == 3 {
                let (packet_buf,_) = socketio::get_packet_from_stream(&mut self.conn);
                if !pack::check_pack(&packet_buf) {
                    self.erro_pack(&packet_buf);
                }else {
                    self.status = true;
                }
            }else {
                self.status = true;
            }

        } else {
            self.erro_pack(&packet_buf);
        }
        Ok(())
    }

    pub fn sha2_auth(&mut self, auth_data: &Vec<u8>, conf: &Config) -> Result<bool, Box<dyn Error>> {
        let (payload, seq_id) = ([0x02],5);
        let mut packet: Vec<u8> = vec![];
        packet.extend(pack_header(&payload, seq_id));
        packet.extend(payload.iter());
        write_value(&mut self.conn, &packet)?;

        let (packet_buf,_) = socketio::get_packet_from_stream(&mut self.conn);

        let key = &packet_buf[1..];
        let mut password = conf.password.as_bytes().to_vec();
        password.push(0);
        for i in 0..password.len() {
            password[i] ^= auth_data[i % auth_data.len()];
        }
        let encrypted_pass = encrypt(&password, &key);
        let mut packet: Vec<u8> = vec![];
        packet.extend(pack_header(&encrypted_pass, 7));
        packet.extend(encrypted_pass.iter());
        write_value(&mut self.conn, &packet)?;

        let (packet_buf,_) = socketio::get_packet_from_stream(&mut self.conn);
        if pack::check_pack(&packet_buf) {
            return Ok(true);
        } else {
            self.erro_pack(&packet_buf);
            return Ok(false);
        }
    }

    fn get_packet_type<F: Read + Seek>(&mut self, buf: &mut F) -> Result<(), Box<dyn Error>>{
        let packet_type = buf.read_u8()?;
        if packet_type == 255{
            self.packet_type = MysqlPacketType::Error;
        }else if packet_type == 10 {
            self.packet_type = MysqlPacketType::Handshake;
        }
        return Ok(())
    }


    /// Type	    Name	            Description
    /// int<1>	    header	            0xFF ERR packet header
    /// int<2>	    error_code	        error-code
    /// if capabilities & CLIENT_PROTOCOL_41 {
    /// string[1]	sql_state_marker	# marker of the SQL state
    /// string[5]	sql_state	        SQL state
    /// }
    /// string<EOF>	error_message	    human readable error message

    pub fn erro_pack(&mut self, pack: &Vec<u8>) {
        self.errorcode = readvalue::read_u16(&pack[1..1+2]) as usize;
        self.error = readvalue::read_string_value(&pack[3..]);

    }
}

//fn conn(host_info: &str) -> Result<TcpStream, Box<dyn Error>> {
//    let tcp_conn = crate::mysql::conn(host_info)?;
////    let tcp_conn = TcpStream::connect(host_info)?;
////    tcp_conn.set_read_timeout(None)?;
////    tcp_conn.set_write_timeout(Some(Duration::new(10,10)))?;
//    Ok(tcp_conn)
//}
//
//pub fn create_mysql_conn(conf: &Config) -> Result<TcpStream, Box<dyn Error>>{
//    //这里是与mysql建立连接的整个过程
//    let mut mysql_conn = conn(&conf.host_info)?;
////    let mut mysql_conn = conn(&conf.host_info).unwrap_or_else(|err|{
////        info!("{}",err);
////        process::exit(1);
////    });
//
//    let (packet_buf,_) = socketio::get_packet_from_stream(&mut mysql_conn);
//    let handshake = pack::HandshakePacket::new(&packet_buf)?;
//
//    //根据服务端发送的hand_shake包组回报并发送
//    let handshake_response = response::LocalInfo::new(conf.program_name.borrow(), conf.database.len() as u8);
//    let packet_type = meta::PackType::HandShakeResponse;
//    let v = response::LocalInfo::pack_payload(
//        &handshake_response,&handshake,&packet_type,conf)?;
//
//    socketio::write_value(&mut mysql_conn, v.as_ref())?;
//
//    //检查服务端回包情况
//    let (packet_buf,_) = socketio::get_packet_from_stream(&mut mysql_conn);
//
//
//    let mut tmp_auth_data = vec![];
//    if packet_buf[0] == 0xFE {
//        //重新验证密码
//        let (auth_data, tmp) = response::authswitchrequest(&handshake, packet_buf.as_ref(), conf);
//        tmp_auth_data = tmp;
//        socketio::write_value(&mut mysql_conn, auth_data.as_ref())?;
//    }
//
//    let (packet_buf,_) = socketio::get_packet_from_stream(&mut mysql_conn);
//    if pack::check_pack(&packet_buf) {
////        use std::{thread, time};
////        let ten_millis = time::Duration::from_secs(100);
////        thread::sleep(ten_millis);
//        if packet_buf[1] == 4{
//            if !response::sha2_auth(&mut mysql_conn, &tmp_auth_data, conf){
//                return Box::new(Err(String::from("connection failed"))).unwrap();
//            }
//        }else if packet_buf[1] == 3 {
//            let (packet_buf,_) = socketio::get_packet_from_stream(&mut mysql_conn);
//            if !pack::check_pack(&packet_buf) {
//                return Box::new(Err(String::from("connection failed"))).unwrap();
//            }
//        }
//        Ok(mysql_conn)
//    } else {
//        return Box::new(Err(String::from("connection failed"))).unwrap();
//    }
//}




