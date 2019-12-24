/*
@author: xiao cai niao
@datetime: 2019/11/11
*/

pub mod mysql;
pub mod pool;
pub mod io;
pub mod readvalue;
pub mod meta;
pub mod binlog;
pub mod storage;

use pool::ThreadPool;
use structopt::StructOpt;
use std::sync::{Arc};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;
use crate::mysql::ReponseErr;

#[macro_use]
extern crate log;
extern crate log4rs;

use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::append::file::FileAppender;
use log4rs::encode::pattern::PatternEncoder;
use log4rs::config::{Appender, Root};
use std::error::Error;

fn init_log() {
    let stdout = ConsoleAppender::builder().build();

    let requests = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} - {m}{n}")))
        .build("log/requests.log")
        .unwrap();

    let config = log4rs::config::Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(Appender::builder().build("requests", Box::new(requests)))
        .logger(log4rs::config::Logger::builder().build("app::backend::db", LevelFilter::Info))
        .logger(log4rs::config::Logger::builder()
            .appender("requests")
            .additive(false)
            .build("app::requests", LevelFilter::Info))
        .build(Root::builder().appender("requests").build(LevelFilter::Info))
        .unwrap();
    log4rs::init_config(config).unwrap();
}

#[derive(Debug, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
pub struct Opt {
    #[structopt(long = "slowlog", help="开启慢日志监控")]
    pub slowlog: bool,

    #[structopt(long = "audit", help="开启审计日志功能")]
    pub audit: bool,

    #[structopt(long = "monitor", help="实时监控")]
    pub monitor: bool,

    #[structopt(long = "port", help="程序运行端口, 默认9011")]
    pub port: Option<String>,

    #[structopt(short = "u", long = "user",help = "mysql用户名")]
    pub user: Option<String>,

    #[structopt(short = "p", long = "password",help = "mysql密码")]
    pub password: Option<String>,

    #[structopt(short = "h",long = "host", help="ip地址加端口, ip:port 例如127.0.0.1:3306")]
    pub host: Option<String>,

    #[structopt(long = "repluser",help = "主从同步用户名")]
    pub repluser: Option<String>,

    #[structopt(long = "replpasswd",help = "主从同步密码")]
    pub replpasswd: Option<String>,

    #[structopt(long = "binlogdir",help = "binlog文件保存路径,默认为/usr/local/mysql/data")]
    pub binlogdir: Option<String>,

}

#[derive(Debug, Clone)]
pub struct Config {
    pub slowlog: bool,
    pub audit: bool,
    pub monitor: bool,
    pub port: u32,
    pub host_info: String,
    pub user_name: String,
    pub password: String,
    pub database: String,
    pub program_name: String,
    pub repl_user: String,
    pub repl_passwd: String,
    pub binlogdir: String
}

impl Config{
    pub fn new(args: Opt) -> Result<Config, &'static str> {
        let mut host_info = String::from("");
        let mut user_name = String::from("");
        let mut password = String::from("");
        let mut repl_user = String::from("");
        let mut repl_passwd = String::from("");
        let mut binlogdir = String::from("/usr/local/mysql/data");
        let database = String::from("");
        let slowlog = args.slowlog;
        let audit = args.audit;
        let monitor = args.monitor;
        let mut port : u32 = 9011;

        match args.binlogdir {
            None => {
            },
            Some(t) => binlogdir = t,
        }

        match args.repluser {
            None => {
                return Err("repluser 不能为空！！");
            },
            Some(t) => repl_user = t,
        }

        match args.replpasswd {
            None => {
                return Err("replpasswd 不能为空！！");
            },
            Some(t) => repl_passwd = t,
        }

        match args.user {
            None => {
                return Err("user 不能为空！！");
            },
            Some(t) => user_name = t,
        }

        match args.host {
            None => {
                return Err("host 不能为空！！");
            },
            Some(t) => host_info = t,
        }

        match args.password {
            None => {
                return Err("password 不能为空！！")
            },
            Some(t) => password = t,
        }

        match args.port {
            Some(t) => {port = t.parse().unwrap()}
            _ => {}
        }

        Ok(Config{
            slowlog,
            audit,
            monitor,
            port,
            user_name,
            host_info,
            password,
            database,
            repl_user,
            repl_passwd,
            program_name:String::from("rust_test"),
            binlogdir
        })
    }

    pub fn clone_new(&self) -> Config {
        Config{
            slowlog: self.slowlog.clone(),
            audit: self.audit.clone(),
            monitor: self.monitor.clone(),
            port: self.port.clone(),
            host_info: self.host_info.clone(),
            user_name: self.user_name.clone(),
            password: self.password.clone(),
            database: self.database.clone(),
            program_name: self.program_name.clone(),
            repl_user: self.repl_user.clone(),
            repl_passwd: self.repl_passwd.clone(),
            binlogdir: self.binlogdir.clone()
        }
    }

    pub fn alter_host(&mut self, host_info: String) {
        self.host_info = host_info;
    }
}


pub fn start(conf: Config) {
    init_log();
    let listen_info = format!("0.0.0.0:{}",conf.port);
    let listener = TcpListener::bind(listen_info).unwrap_or_else(|err|{
        info!("{:?}",err);
        std::process::exit(1)
    });

    //mysql 状态检查线程
    let conf = Arc::new(conf);
//    let mysql_state = Arc::new(Mutex::new(mysql::state_check::MysqlState::new()));
//    thread::spawn(move|| {
//        let conf = Arc::clone(&conf);
//        let mysql_state = Arc::clone(&mysql_state);
//        mysql::state_check::mysql_state_check(conf, mysql_state);
//    });

    let pool = ThreadPool::new(4);
    // accept connections and process them serially
    for stream in listener.incoming() {
        let conf = Arc::clone(&conf);
        let stream = stream.unwrap();
        pool.execute(move||{
            handle_stream(stream, conf)
        });
    }
}


fn handle_stream(mut tcp: TcpStream, conf: Arc<Config>) {
    tcp.set_read_timeout(Some(Duration::new(2,10))).expect("set_read_timeout call failed");
    tcp.set_write_timeout(Some(Duration::new(10,10))).expect("set_write_timeout call failed");

    let result = readvalue::rec_packet(&mut tcp);
//    let mut buf = [0u8; 1];
//    let result = tcp.read_exact(&mut buf);
    //println!("{}",buf[0]);
    match result {
        Ok(buf) => {
            let type_code = mysql::MyProtocol::new(&buf[0]);
            //println!("{:?},{:?}",buf[0],type_code);
            match type_code {
                mysql::MyProtocol::MysqlCheck => {
                    let mysql_status = mysql::state_check::mysql_state_check(conf);
                    let state = mysql::send_value_packet(&tcp, &mysql_status, mysql::MyProtocol::MysqlCheck);
                    match state {
                        Ok(()) => {}
                        Err(e) => {
                            info!("{}",e.to_string());
                        }
                    }
                }
                mysql::MyProtocol::GetSlowLog => {}
                mysql::MyProtocol::GetAuditLog => {}
                mysql::MyProtocol::GetMonitor => {}
                mysql::MyProtocol::SetVariables => {
                    if let Err(e) = mysql::changemaster::set_variabels(&mut tcp, &conf){
                        let state = mysql::send_error_packet(&ReponseErr::new(e.to_string()), &mut tcp);
                        mysql::check_state(&state);
                        info!("{}",e.to_string());
                        return;
                    };
                }
                mysql::MyProtocol::SetMaster => {
                    info!("myself is new master...");
                    let state = mysql::setmaster::set_master(&tcp, &conf);
                    mysql::check_state(&state);
                }
                mysql::MyProtocol::ChangeMaster => {
                    info!("change master packet...");
                    let state = mysql::changemaster::change_master(&tcp, &conf, &buf);
                    mysql::check_state(&state);
                }
                mysql::MyProtocol::PullBinlog => {
                    if let Err(e) = mysql::syncbinlog::pull_binlog_info(&conf, &mut tcp, &buf){
                        info!("{}", &e.to_string());
                        let state = mysql::send_error_packet(&ReponseErr::new(e.to_string()), &mut tcp);
                        mysql::check_state(&state);
                    };
                }
                mysql::MyProtocol::PushBinlog => {
                    if let Err(e) = mysql::syncbinlog::push_binlog_info(&conf, &mut tcp, &buf){
                        info!("{}", &e.to_string());
                        let state = mysql::send_error_packet(&ReponseErr::new(e.to_string()), &mut tcp);
                        mysql::check_state(&state);
                    };
                }
                mysql::MyProtocol::RecoveryCluster => {
                    info!("this is a recoverycluster packet !!");
                    let state = mysql::recovery::recovery_my_slave(&mut tcp, &conf, &buf);
                    match state {
                        Ok(()) => {
                            info!("recovery down ");
                        }
                        Err(e) => {
                            info!("{:?}",e.to_string());
                        }
                    }
                }
                mysql::MyProtocol::GetRecoveryInfo => {
                    let mut recovery_info = mysql::recovery::GetRecoveryInfo::new();
                    if let Err(e) = recovery_info.get_state(&conf){
                        let err = e.to_string();
                        let state = mysql::send_error_packet(&ReponseErr::new(err.clone()), &mut tcp);
                        mysql::check_state(&state);
                        info!("{}",err);
                        return;
                    }
                    let state = mysql::send_value_packet(&tcp, &recovery_info, mysql::MyProtocol::GetRecoveryInfo);
                    mysql::check_state(&state);
                }
                mysql::MyProtocol::DownNodeCheck => {
                    let state = mysql::nodecheck::check_down_node(&mut tcp, &conf, &buf);
                    mysql::check_state(&state);
                }
                mysql::MyProtocol::Command => {
                    if let Err(e) = mysql::push_sql::push_sql_to_db(&mut tcp, &conf, &buf){
                        info!("{}", &e.to_string());
                        let state = mysql::send_error_packet(&ReponseErr::new(e.to_string()), &mut tcp);
                        mysql::check_state(&state);
                    }

                }

                mysql::MyProtocol::UnKnow => {
                    let err = ReponseErr{err:String::from("Invalid type_code")};
                    //let value = serde_json::to_string(&err).unwrap();
                    let state = mysql::send_value_packet(&tcp, &err, mysql::MyProtocol::Error);
                    match state {
                        Ok(()) => {}
                        Err(e) => {
                            info!("{}",e);
                        }
                    }
                }
                _ => {}
            }
        }
        Err(e) => {
            info!("read packet from tcp failed: {:?}", e);
            let state = mysql::send_error_packet(&ReponseErr::new(e.to_string()), &mut tcp);
            mysql::check_state(&state);
        }
    }

}


fn create_conn(config: &Config) -> Result<TcpStream, Box<dyn Error>> {
    let conn = io::connection::create_mysql_conn(config)?;
    return Ok(conn);
}
