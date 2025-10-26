//! A single threaded executor that uses shortest-job-first scheduling.
//!
//! 单线程优先级执行器
//! - 按照上次轮询(poll)消耗时间的升序排列顺序执行任务。
//! - 新任务，消耗时间默认为0，优先级最高，排在队列头部。
//!
//! 如何记录任务上次轮询消耗的时间：
//! - 为每个任务附加一个记录消耗时间的元数据。
//! - 重写一个future生成函数，包装了旧的future和对任务元数据(轮询耗时)的引用。
//! - 新的future在轮询自身时，计算轮询耗时，通过所带的引用，将耗时更新到任务元数据中。
//!
//! 如何进行调度实现优先级：
//! - 任务包装ByDuration对象，为任务附加排序能力。这里是依据元数据中的事件升序。
//! - 任务被调度时，包装为ByDuration，插入优先级队列，依据任务元数据携带的消耗时间升序排列。
//! - 执行器执行时，从队列头部开始执行任务，耗时最小的最先执行。

use std::cell::RefCell;
use std::collections::BinaryHeap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::thread;
use std::time::{Duration, Instant};
use std::{cell::Cell, future::Future};

use async_task::{Builder, Runnable, Task};
use pin_project_lite::pin_project;
use smol::{channel, future};

/// 任务Runnable的包装，为了实现任务能按耗时升序插入优先级队列。
struct ByDuration(Runnable<DurationMetadata>);

impl ByDuration {
    fn duration(&self) -> Duration {
        self.0.metadata().inner.get()
    }
}

impl PartialEq for ByDuration {
    fn eq(&self, other: &Self) -> bool {
        self.duration() == other.duration()
    }
}

impl Eq for ByDuration {}

impl PartialOrd for ByDuration {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ByDuration {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.duration().cmp(&other.duration()).reverse() // 升序
    }
}

pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    struct MeasureRuntime<'a, F> {
        #[pin]
        f: F,
        duration: &'a Cell<Duration>
    }
}

impl<F: Future> Future for MeasureRuntime<'_, F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let duration_cell: &Cell<Duration> = this.duration;
        let start = Instant::now();
        let res = F::poll(this.f, cx);
        // 计算轮询耗时，通过引用更新到future所在任务的元数据中。
        let new_duration = Instant::now() - start;
        duration_cell.set(duration_cell.get() / 2 + new_duration / 2);
        res
    }
}

pub struct DurationMetadata {
    inner: Cell<Duration>,
}

thread_local! {
    /// 全局任务优先级队列
    /// - 实际存储：任务做了一层包装ByDuration对象，以此为任务附加排序能力。
    /// - 排序依据：依据任务携带的元数据中的DurationMetadata，进行升序。
    static QUEUE: RefCell<BinaryHeap<ByDuration>> = RefCell::new(BinaryHeap::new());
}

/// 创建新的future生成函数，替代原有的生成函数。
///
/// 主要目的：
/// - 让新生成的future自身可以携带任务元数据，在执行时可以利用元数据做一些逻辑判断。
/// - 比如这里，元数据为DurationMetadata，嵌入新future后，可用于自身执行时做事件跨度判断。
fn make_future_fn<'a, F>(
    future: F,
) -> impl (FnOnce(&'a DurationMetadata) -> MeasureRuntime<'a, F>) {
    move |duration_meta| MeasureRuntime {
        f: future,
        duration: &duration_meta.inner,
    }
}

/// 断言函数，确保调度函数符合Send，Sync，'static三个条件。
fn ensure_safe_schedule<F: Send + Sync + 'static>(f: F) -> F {
    f
}

/// Spawns a future on the executor.
///
/// 创建新任务并调度到执行器上。
///
/// 此函数与原始的spawn的变化：
/// - future生成函数：为原始的future附加了任务元数据DurationMetadata，生成新的MeasureRuntime。
/// - 调度器：自定义了一个调度器，添加了是否为同一线程的判断。
pub fn spawn<F, T>(future: F) -> Task<T, DurationMetadata>
where
    F: Future<Output = T> + 'static,
    T: 'static,
{
    // 获取当前线程ID
    let spawn_thread_id = thread::current().id();
    // Create a task that is scheduled by pushing it into the queue.
    // 调度函数：调度时必须确保与孵化线程处于同一线程。
    // 调度逻辑：将任务包装后插入全局队列。
    let schedule = ensure_safe_schedule(move |runnable| {
        if thread::current().id() != spawn_thread_id {
            panic!("Task would be run on a different thread than spawned on.");
        }
        QUEUE.with(move |queue| queue.borrow_mut().push(ByDuration(runnable)));
    });

    // 创建新的future生成函数，捕获了旧的future：
    // - 新逻辑：将任务元数据与旧future捆绑为新的future，即MeasureRuntime。
    // - 入参：DurationMetadata，任务的元数据。
    // - 出参：MeasureRuntime，是新的future。
    let future_fn = make_future_fn(future);

    // 创建任务
    // - 任务元数据：DurationMetadata。
    // - future生成函数：将最初始的future和任务元数据捆绑为新的MeasureRuntime。
    // - 调度器：调度时额外添加了同一线程判断，最终插入全局队列。
    let (runnable, task) = unsafe {
        Builder::new()
            .metadata(DurationMetadata {
                inner: Cell::new(Duration::default()),
            })
            .spawn_unchecked(future_fn, schedule)
    };

    // Schedule the task by pushing it into the queue.
    // 调度一次任务自身。
    runnable.schedule();

    // 返回任务等待句柄。
    task
}

pub fn block_on<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    let task = spawn(future);
    while !task.is_finished() {
        let Some(runnable) = QUEUE.with(|queue| queue.borrow_mut().pop()) else {
            thread::yield_now();
            continue;
        };
        runnable.0.run();
    }
}

fn main() {
    // Spawn a future and await its result.
    block_on(async {
        let (sender, receiver) = channel::bounded(1);
        let world = spawn(async move {
            receiver.recv().await.unwrap();
            println!("world.")
        });
        let hello = spawn(async move {
            sender.send(()).await.unwrap();
            print!("Hello, ")
        });
        future::zip(hello, world).await;
    });
}
