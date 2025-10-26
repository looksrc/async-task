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
/// - 创建任务：利用传入的future创建一个异步任务实例。
/// - 执行器：开辟线程循环获取并执行任务。任务获取：从通道拉取异步任务。
/// - 调度器：将异步任务推入通道发给执行器。
///
/// 这个例子的启发：
/// - 调度器：
///   - 作用：是让任务自身可以再次得到执行。(本利是将任务再次发给执行器线程即可再次得到执行)
///   - 使用：1.手动调度 2.任务的IO就绪后的Waker任务唤醒逻辑中调度
/// - 任务对象对外提供了几个接口，可供外部集成：创建、执行、调度。
/// - 任务对象是可移植的，可被任意的执行器执行，只需要为任务提供与执行器相匹配的调度器。
fn spawn<F, T>(future: F) -> Task<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    // A queue that holds scheduled tasks.
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
    // - 2.一个异步版任务并附加调度器，返回任务的执行句柄、任务的监测句柄。
    let schedule = |runnable| QUEUE.send(runnable).unwrap();
    let (runnable, task) = async_task::spawn(future, schedule);

    // Schedule the task by pushing it into the queue.
    // 异步任务调度一次自己。(这里是插入任务执行队列)
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
