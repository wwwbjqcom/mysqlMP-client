/*
@author: xiao cai niao
@datetime: 2019/11/11
*/
use std::thread;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;

trait FnBox{
    fn call_box(self: Box<Self>);
}
impl<F: FnOnce()> FnBox for F {
    fn call_box(self: Box<Self>) {
        (*self)()
    }
}

type Job = Box<dyn FnBox + Send + 'static>;

enum Message {
    NewJob(Job),
    Terminate,
}

pub struct ThreadPool{
    threads: Vec<Worker>,
    sender: mpsc::Sender<Message>
}

struct Worker{
    id: usize,
    thread: Option<thread::JoinHandle<()>>
}

impl Worker{
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Worker{
        ///创建worker实例
        ///
        /// 一个worker实例对应一个id和未传递任何参数的线程
        ///
        /// id: 线程id
        /// receiver: 接收任务的channel
        let thread = thread::spawn(move ||{
            loop {
                let msg = receiver.lock().unwrap().recv().unwrap();
                println!("Worker {} got a job; executing.", id);
                match msg {
                    Message::NewJob(job) => {
                        job.call_box();
                    }
                    Message::Terminate => {
                        println!("Worker {} was told to terminate.", id);
                        break;
                    }
                }
            }
        });
        Worker {
            id,
            thread: Some(thread)
        }
    }
}

impl ThreadPool{
    pub fn new(size: usize) -> ThreadPool {
        /// 创建线程池。
        ///
        /// 线程池中线程的数量。
        ///
        /// # Panics
        ///
        /// `new` 函数在 size 为 0 时会 panic。
        assert!(size > 0);

        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let mut threads = Vec::with_capacity(size);
        for i in 0..size {
            //创建线程并存放于threads中
            threads.push(Worker::new(i, Arc::clone(&receiver)));
        }
        ThreadPool{
            threads,
            sender
        }
    }
    pub fn execute<F> (&self, f: F)
        where
            F: FnOnce() + Send + 'static{
        ///执行任务
        ///
        /// 通过channel分发任务
        let msg = Message::NewJob(Box::new(f));
        self.sender.send(msg).unwrap();
    }
}

impl Drop for ThreadPool{
    fn drop(&mut self) {
        for _ in &mut self.threads{
            self.sender.send(Message::Terminate).unwrap();
        }

        println!("Shutting down all workers.");

        for worker in &mut self.threads {
            println!("Shutting down worker {}", worker.id);
            if let Some(thread) = worker.thread.take(){
                thread.join().unwrap();
            }
        }
    }
}
