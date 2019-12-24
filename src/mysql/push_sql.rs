/*
@author: xiao cai niao
@datetime: 2019/12/21
*/

use std::net::TcpStream;
use std::sync::Arc;
use crate::Config;
use std::error::Error;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct CommandSql{
    pub sqls: Vec<String>
}

impl CommandSql{
    fn execute(&self, conn: &mut TcpStream) -> Result<(), Box<dyn Error>>{
        self.start_traction(conn)?;
        for sql in &self.sqls{
            crate::io::command::execute_update(conn, &sql)?;
        }
        self.commit(conn)?;
        Ok(())
    }

    fn start_traction(&self,conn: &mut TcpStream) -> Result<(), Box<dyn Error>>{
        crate::io::command::execute_update(conn, &"begin;".to_string())?;
        Ok(())
    }
    fn commit(&self, conn: &mut TcpStream) -> Result<(), Box<dyn Error>>{
        crate::io::command::execute_update(conn, &"commit;".to_string())?;
        Ok(())
    }
}

pub fn push_sql_to_db(tcp: &mut TcpStream, conf: &Arc<Config>, buf: &Vec<u8>) -> Result<(), Box<dyn Error>> {
    let mut conn = crate::create_conn(conf)?;
    let value = &buf[9..];
    let value: CommandSql = serde_json::from_slice(&value).unwrap();
    value.execute(&mut conn)?;
    crate::mysql::send_ok_packet(tcp)?;
    Ok(())
}

//pub fn test(conf: &Config, value: &CommandSql) -> Result<(), Box<dyn Error>> {
//    let mut conn = crate::create_conn(conf)?;
//    value.execute(&mut conn)?;
//    Ok(())
//}