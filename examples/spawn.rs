//! A simple single-threaded executor.
//! 一个简单的单线程执行器。

use std::future::Future;
use std::panic::catch_unwind;
use std::thread;

use async_task::{Runnable, Task};
use once_cell::sync::Lazy;
use smol::future;

/// Spawns a future on the executor.
/// 在执行器上孵化一个Future。
fn spawn<F, T>(future: F) -> Task<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    // A queue that holds scheduled tasks.
    // 持有已被调度任务的队列。发送端是全局的，接收端在执行器线程中。
    static QUEUE: Lazy<flume::Sender<Runnable>> = Lazy::new(|| {
        let (sender, receiver) = flume::unbounded::<Runnable>();

        // Start the executor thread.
        // 启动执行器线程。
        thread::spawn(|| {
            for runnable in receiver {
                // Ignore panics inside futures.
                // 忽略Future内部的恐慌。
                let _ignore_panic = catch_unwind(|| runnable.run());
            }
        });

        sender
    });

    // Create a task that is scheduled by pushing it into the queue.
    // 创建一个任务，对其进行调度(插入被调度的任务队列)。
    // - 1.创建调度器(Schedule)实例schedule
    // - 2.孵化一个Future任务并为其绑定一个调度器，返回任务的执行句柄、任务的监测句柄。
    let schedule = |runnable| QUEUE.send(runnable).unwrap();
    let (runnable, task) = async_task::spawn(future, schedule);

    // Schedule the task by pushing it into the queue.
    // 利用任务绑定的调度器来调度任务。(这里是插入任务执行队列)
    runnable.schedule();

    task
}

fn main() {
    // Spawn a future and await its result.
    // 孵化一个Future任务
    let task = spawn(async {
        println!("Hello, world!");
    });

    // 阻塞等待监测句柄监测到任务已完成。
    future::block_on(task);
}
