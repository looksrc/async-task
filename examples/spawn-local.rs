//! A simple single-threaded executor that can spawn non-`Send` futures.

use std::cell::Cell;
use std::future::Future;
use std::rc::Rc;

use async_task::{Runnable, Task};

thread_local! {
    // A queue that holds scheduled tasks.
    static QUEUE: (flume::Sender<Runnable>, flume::Receiver<Runnable>) = flume::unbounded();
}

/// Spawns a future on the executor.
///
/// 创建一个包裹future的异步任务，并调度到执行器一次，最后返回任务的等待句柄。
fn spawn<F, T>(future: F) -> Task<T>
where
    F: Future<Output = T> + 'static,
    T: 'static,
{
    // Create a task that is scheduled by pushing itself into the queue.
    let schedule = |runnable| QUEUE.with(|(s, _)| s.send(runnable).unwrap());
    let (runnable, task) = async_task::spawn_local(future, schedule);

    // Schedule the task by pushing it into the queue.
    runnable.schedule();

    task
}

/// Runs a future to completion.
///
/// 执行一个future直到其执行完成，函数才返回：
/// - 第一步：孵化并调度一个后台异步任务，任务功能是执行future并将其结果发送出来。
/// - 第二步：循环。先尝试收取future的执行结果，收成功则返回，没收到则驱动一次执行器。
fn run<F, T>(future: F) -> T
where
    F: Future<Output = T> + 'static,
    T: 'static,
{
    // Spawn a task that sends its result through a channel.
    // 传递future执行结果的通道。
    let (s, r) = flume::unbounded();
    spawn(async move { drop(s.send(future.await)) }).detach();

    loop {
        // If the original task has completed, return its result.
        // 尝试读取结果。
        if let Ok(val) = r.try_recv() {
            return val;
        }

        // Otherwise, take a task from the queue and run it.
        // 执行器逻辑。
        QUEUE.with(|(_, r)| r.recv().unwrap().run());
    }
}

fn main() {
    // 通过在任务中放入一个不能跨线程类型的val变量，来验证spawn_local。
    let val = Rc::new(Cell::new(0));

    // Run a future that increments a non-`Send` value.
    run({
        let val = val.clone();
        async move {
            // Spawn a future that increments the value.
            let task = spawn({
                let val = val.clone();
                async move {
                    val.set(dbg!(val.get()) + 1);
                }
            });

            val.set(dbg!(val.get()) + 1);
            task.await;
        }
    });

    // The value should be 2 at the end of the program.
    dbg!(val.get());
}
