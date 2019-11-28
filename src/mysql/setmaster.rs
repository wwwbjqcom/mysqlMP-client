/*
@author: xiao cai niao
@datetime: 2019/11/11
*/

use std::net::TcpStream;
use crate::Config;
use std::sync::Arc;
use crate::mysql;
use std::error::Error;

pub fn set_master(mut tcp: &TcpStream, conf: &Arc<Config>) {
    let conn = crate::create_conn(conf);
    match conn {
        Ok(mut tcp) => {
            let state = set_master_info(&mut tcp);
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
}

///
/// 当该节点被选举为master，执行重置slave线程并把readonly和flush参数重置
///
fn set_master_info(tcp: &mut TcpStream) -> Result<(), Box<dyn Error>> {
    let stop_slave_sql = String::from("stop slave all;");
    let reset_slave_sql = String::from("reset slave all;");
    crate::io::command::execute_update(tcp, &stop_slave_sql)?;
    crate::io::command::execute_update(tcp, &reset_slave_sql)?;
    mysql::set_no_readonly(tcp)?;
    Ok(())
}
