use mymha_client;
use structopt::StructOpt;

//use std::net::{TcpListener, TcpStream};
//
//fn handle_client(stream: TcpStream) {
//    // ...
//}

fn main() {
    let args = mymha_client::Opt::from_args();
    let conf = mymha_client::Config::new(args).unwrap_or_else(|err|{
        println!("Problem parsing arguments: {}", err);
        std::process::exit(1);
    });
    //println!("{:?}",conf);

    mymha_client::start(conf);

//    let sqls = CommandSql{ sqls: vec!["insert into xz_test.t1 values(1),(2),(3)".to_string()] };
//    test(&conf, &sqls).unwrap()


//    let listener = TcpListener::bind("127.0.0.1:80")?;
//
//    // accept connections and process them serially
//    for stream in listener.incoming() {
//        handle_client(stream?);
//    }
//    Ok(())
}
