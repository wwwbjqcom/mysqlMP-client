/*
@author: xiao cai niao
@datetime: 2019/11/11
*/

use std::net::TcpStream;
use std::sync::Arc;
use crate::Config;
use std::error::Error;
use std::collections::HashMap;
use crate::mysql::MyProtocol;
use serde::Serialize;
use serde::Deserialize;

///
/// mysql运行状态监控值
#[derive(Deserialize, Serialize, Debug)]
pub struct MysqlMonitorStatus{
    pub com_insert: usize,
    pub com_update: usize,
    pub com_delete: usize,
    pub com_select: usize,
    pub questions: usize,
    pub innodb_row_lock_current_waits: usize,
    pub innodb_row_lock_time: usize,
    pub created_tmp_disk_tables: usize,
    pub created_tmp_tables: usize,
    pub innodb_buffer_pool_reads: usize,
    pub innodb_buffer_pool_read_requests: usize,
    pub handler_read_first: usize,
    pub handler_read_key: usize,
    pub handler_read_next: usize,
    pub handler_read_prev: usize,
    pub handler_read_rnd: usize,
    pub handler_read_rnd_next: usize,
    pub innodb_os_log_pending_fsyncs: usize,
    pub innodb_os_log_pending_writes: usize,
    pub innodb_log_waits: usize,
    pub threads_connected: usize,
    pub threads_running: usize,
    pub bytes_sent: usize,
    pub bytes_received: usize,
    pub slow_queries: usize,
    pub time: i64
}

impl MysqlMonitorStatus{
    pub fn new() -> MysqlMonitorStatus {
        MysqlMonitorStatus{
            com_insert: 0,
            com_update: 0,
            com_delete: 0,
            com_select: 0,
            questions: 0,
            innodb_row_lock_current_waits: 0,
            innodb_row_lock_time: 0,
            created_tmp_disk_tables: 0,
            created_tmp_tables: 0,
            innodb_buffer_pool_reads: 0,
            innodb_buffer_pool_read_requests: 0,
            handler_read_first: 0,
            handler_read_key: 0,
            handler_read_next: 0,
            handler_read_prev: 0,
            handler_read_rnd: 0,
            handler_read_rnd_next: 0,
            innodb_os_log_pending_fsyncs: 0,
            innodb_os_log_pending_writes: 0,
            innodb_log_waits: 0,
            threads_connected: 0,
            threads_running: 0,
            bytes_sent: 0,
            bytes_received: 0,
            slow_queries: 0,
            time: 0
        }
    }



    pub fn parse_value(&mut self, result: &Vec<HashMap<String,String>>) -> Result<(), Box<dyn Error>> {
        self.time = crate::timestamp();
        for row in result{
            let var_name = row.get(&String::from("Variable_name")).unwrap();
            let value = row.get(&String::from("Value")).unwrap();
            //info!("{},{}", &var_name, &value);
            if var_name == &String::from("Com_insert"){
                self.com_insert = value.parse()?;
            }
            else if var_name ==  &String::from("Com_update"){
                self.com_update = value.parse()?;
            }
            else if var_name == &String::from("Com_delete"){
                self.com_delete = value.parse()?;
            }
            else if var_name ==  &String::from("Com_select"){
                self.com_select = value.parse()?;
            }
            else if var_name == &String::from("Questions"){
                self.questions = value.parse()?;
            }
            else if var_name ==  &String::from("Innodb_row_lock_current_waits"){
                self.innodb_row_lock_current_waits = value.parse()?;
            }
            else if var_name == &String::from("Innodb_row_lock_time"){
                self.innodb_row_lock_time = value.parse()?;
            }
            else if var_name == &String::from("Created_tmp_disk_tables"){
                self.created_tmp_disk_tables = value.parse()?;
            }
            else if var_name == &String::from("Created_tmp_tables"){
                self.created_tmp_tables = value.parse()?;
            }
            else if var_name == &String::from("Innodb_buffer_pool_reads"){
                self.innodb_buffer_pool_reads = value.parse()?;
            }
            else if var_name == &String::from("Innodb_buffer_pool_read_requests"){
                self.innodb_buffer_pool_read_requests = value.parse()?;
            }
            else if var_name == &String::from("Handler_read_first"){
                self.handler_read_first = value.parse()?;
            }
            else if var_name == &String::from("Handler_read_key"){
                self.handler_read_key = value.parse()?;
            }
            else if var_name == &String::from("Handler_read_next"){
                self.handler_read_next = value.parse()?;
            }
            else if var_name == &String::from("Handler_read_prev"){
                self.handler_read_prev = value.parse()?;
            }
            else if var_name == &String::from("Handler_read_rnd"){
                self.handler_read_rnd = value.parse()?;
            }
            else if var_name == &String::from("Handler_read_rnd_next"){
                self.handler_read_rnd_next = value.parse()?;
            }
            else if var_name == &String::from("Innodb_os_log_pending_fsyncs"){
                self.innodb_os_log_pending_fsyncs = value.parse()?;
            }
            else if var_name == &String::from("Innodb_os_log_pending_writes"){
                self.innodb_os_log_pending_writes = value.parse()?;
            }
            else if var_name == &String::from("Innodb_log_waits"){
                self.innodb_log_waits = value.parse()?;
            }
            else if var_name == &String::from("Threads_connected"){
                self.threads_connected = value.parse()?;
            }
            else if var_name == &String::from("Threads_running"){
                self.threads_running = value.parse()?;
            }
            else if var_name == &String::from("Bytes_sent"){
                self.bytes_sent = value.parse()?;
            }
            else if var_name == &String::from("Bytes_received"){
                self.bytes_received = value.parse()?;
            }
            else if var_name == &String::from("Slow_queries") {
                self.slow_queries = value.parse()?;
            }
        }

        Ok(())
    }

}


pub fn mysql_monitor(tcp: &TcpStream, conf: &Arc<Config>) -> Result<(), Box<dyn Error>>{
    let mut conn = crate::create_conn(&conf)?;
    let mut mysql_status = MysqlMonitorStatus::new();
    let result = crate::io::command::execute(&mut conn, &"show global status".to_string())?;
    if result.len() > 0{
        mysql_status.parse_value(&result)?;
        crate::io::command::close(&mut conn);
        crate::mysql::send_value_packet(tcp, &mysql_status, MyProtocol::GetMonitor)?;
    }else {
        let err = String::from("get global status failed");
        return Err(err.into())
    }
    Ok(())
}
