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
use std::io::{Read};
use std::time::Duration;
use crate::mysql::ReponseErr;

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
}


pub fn start(conf: Config) {
    let listen_info = format!("0.0.0.0:{}",conf.port);
    let listener = TcpListener::bind(listen_info).unwrap_or_else(|err|{
        println!("{:?}",err);
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
    let mut buf = [0u8; 1];
    let result = tcp.read_exact(&mut buf);
    //println!("{}",buf[0]);
    match result {
        Ok(_v) => {}
        Err(e) => {
            println!("read packet from tcp failed: {:?}", e);
            return;
        }
    }

    let type_code = mysql::MyProtocol::new(buf[0]);
    //println!("{:?},{:?}",buf[0],type_code);
    match type_code {
        mysql::MyProtocol::MysqlCheck => {
            let mysql_status = mysql::state_check::mysql_state_check(conf);
            //let value = serde_json::to_string(&mysql_status).unwrap();
            let state = mysql::write_value(&tcp, &mysql_status);
            match state {
                Ok(()) => {}
                Err(e) => {
                    println!("{}",e);
                }
            }
        }
        mysql::MyProtocol::GetSlowLog => {}
        mysql::MyProtocol::GetAuditLog => {}
        mysql::MyProtocol::GetMonitor => {}
        mysql::MyProtocol::SetMaster => {
            mysql::setmaster::set_master(&tcp, &conf);
        }
        mysql::MyProtocol::ChangeMaster => {
            let state = mysql::changemaster::change_master(&tcp, &conf);
            mysql::check_state(&state);
        }
        mysql::MyProtocol::SyncBinlog => {}
        mysql::MyProtocol::RecoveryCluster => {
            println!("this is a recoverycluster packet !!");
            let state = mysql::recovery::recovery_my_slave(&mut tcp, &conf);
            match state {
                Ok(()) => {
                    println!("recovery down ");
                }
                Err(e) => {
                    println!("{:?}",e);
                }
            }
        }
        mysql::MyProtocol::UnKnow => {
            let err = ReponseErr{err:String::from("Invalid type_code")};
            //let value = serde_json::to_string(&err).unwrap();
            let state = mysql::write_value(&tcp, &err);
            match state {
                Ok(()) => {}
                Err(e) => {
                    println!("{}",e);
                }
            }
        }
        _ => {}
    }

}


fn create_conn(config: &Config) -> Result<TcpStream, &'static str> {
    let conn = io::connection::create_mysql_conn(config);
    return conn;
}
