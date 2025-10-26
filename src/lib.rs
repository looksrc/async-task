//! Task abstraction for building executors.
//!
//! To spawn a future onto an executor, we first need to allocate it on the heap and keep some
//! state attached to it. The state indicates whether the future is ready for polling, waiting to
//! be woken up, or completed. Such a stateful future is called a *task*.
//!
//! 构建执行器所需的任务抽象。
//!
//! 要想在执行器上孵化一个future，首先需要在堆上申请内存并将一些状态附加给它。
//! 状态指示了future是否准备好轮询、等待唤醒、完成。
//! 这样的有状态的future，称为"任务"。
//!
//! All executors have a queue that holds scheduled tasks:
//!
//! 所有执行器都有一个持有已调度任务的队列：
//!
//! ```
//! let (sender, receiver) = flume::unbounded();
//! #
//! # // A future that will get spawned.
//! # let future = async { 1 + 2 };
//! #
//! # // A function that schedules the task when it gets woken up.
//! # let schedule = move |runnable| sender.send(runnable).unwrap();
//! #
//! # // Create a task.
//! # let (runnable, task) = async_task::spawn(future, schedule);
//! ```
//!
//! A task is created using either [`spawn()`], [`spawn_local()`], or [`spawn_unchecked()`] which
//! return a [`Runnable`] and a [`Task`]:
//!
//! 任务通常通过[`spawn()`], [`spawn_local()`], [`spawn_unchecked()`]，创建并返回[`Runnable`] and a [`Task`]：
//!
//! ```
//! # let (sender, receiver) = flume::unbounded();
//! #
//! // A future that will be spawned.
//! let future = async { 1 + 2 };
//!
//! // A function that schedules the task when it gets woken up.
//! let schedule = move |runnable| sender.send(runnable).unwrap();
//!
//! // Construct a task.
//! let (runnable, task) = async_task::spawn(future, schedule);
//!
//! // Push the task into the queue by invoking its schedule function.
//! runnable.schedule();
//! ```
//!
//! The [`Runnable`] is used to poll the task's future, and the [`Task`] is used to await its
//! output.
//!
//! [`Runnable`]用于轮询任务future，[`Task`]用于等待输出。
//!
//! Finally, we need a loop that takes scheduled tasks from the queue and runs them:
//!
//! 最终，我们需要一个循环，从队列获取已调度任务并执行他们。
//!
//! ```no_run
//! # let (sender, receiver) = flume::unbounded();
//! #
//! # // A future that will get spawned.
//! # let future = async { 1 + 2 };
//! #
//! # // A function that schedules the task when it gets woken up.
//! # let schedule = move |runnable| sender.send(runnable).unwrap();
//! #
//! # // Create a task.
//! # let (runnable, task) = async_task::spawn(future, schedule);
//! #
//! # // Push the task into the queue by invoking its schedule function.
//! # runnable.schedule();
//! #
//! for runnable in receiver {
//!     runnable.run();
//! }
//! ```
//!
//! Method [`run()`][`Runnable::run()`] polls the task's future once. Then, the [`Runnable`]
//! vanishes and only reappears when its [`Waker`][`core::task::Waker`] wakes the task, thus
//! scheduling it to be run again.
//!
//! 方法[`run()`][`Runnable::run()`]轮询一次任务future。
//! 然后[`Runnable`]被清除，并在[`Waker`][`core::task::Waker`]唤醒任务时再次出现，因此调度它再次执行。

#![no_std]
#![warn(missing_docs, missing_debug_implementations, rust_2018_idioms)]
#![doc(test(attr(deny(rust_2018_idioms, warnings))))]
#![doc(test(attr(allow(unused_extern_crates, unused_variables))))]
#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/smol-rs/smol/master/assets/images/logo_fullsize_transparent.png"
)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/smol-rs/smol/master/assets/images/logo_fullsize_transparent.png"
)]

extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

/// We can't use `?` in const contexts yet, so this macro acts
/// as a workaround.
///
/// 短路，同`?`，区别是可以用在常上下文中。
///
/// 短路效果：直接返回
macro_rules! leap {
    ($x: expr) => {{
        match ($x) {
            Some(val) => val,
            None => return None,
        }
    }};
}

/// 短路，同`?`，区别是可以用在常上下文中。
///
/// 短路效果：抛出恐慌
macro_rules! leap_unwrap {
    ($x: expr) => {{
        match ($x) {
            Some(val) => val,
            None => panic!("called `Option::unwrap()` on a `None` value"),
        }
    }};
}

mod header;
mod raw;
mod runnable;
mod state;
mod task;
mod utils;

pub use crate::runnable::{
    spawn, spawn_unchecked, Builder, Runnable, Schedule, ScheduleInfo, WithInfo,
};
pub use crate::task::{FallibleTask, Task};

#[cfg(feature = "std")]
pub use crate::runnable::spawn_local;
