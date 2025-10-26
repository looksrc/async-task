use alloc::alloc::Layout as StdLayout;
use core::cell::UnsafeCell;
use core::future::Future;
use core::mem::{self, ManuallyDrop};
use core::pin::Pin;
use core::ptr::NonNull;
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

#[cfg(not(feature = "portable-atomic"))]
use core::sync::atomic::AtomicUsize;
use core::sync::atomic::Ordering;
#[cfg(feature = "portable-atomic")]
use portable_atomic::AtomicUsize;

use crate::header::Header;
use crate::runnable::{Schedule, ScheduleInfo};
use crate::state::*;
use crate::utils::{abort, abort_on_panic, max, Layout};
use crate::Runnable;

#[cfg(feature = "std")]
pub(crate) type Panic = alloc::boxed::Box<dyn core::any::Any + Send + 'static>;

#[cfg(not(feature = "std"))]
pub(crate) type Panic = core::convert::Infallible;

/// The vtable for a task.
/// 任务的虚表。
pub(crate) struct TaskVTable {
    /// Schedules the task.
    /// 调度任务。
    pub(crate) schedule: unsafe fn(*const (), ScheduleInfo),

    /// Drops the future inside the task.
    /// 遗弃任务中的Future。
    pub(crate) drop_future: unsafe fn(*const ()),

    /// Returns a pointer to the output stored after completion.
    /// 返回指向任务输出结果的指针。
    pub(crate) get_output: unsafe fn(*const ()) -> *const (),

    /// Drops the task reference (`Runnable` or `Waker`).
    /// 遗弃任务引用。(来自`Runnable`或`Waker`)。
    pub(crate) drop_ref: unsafe fn(ptr: *const ()),

    /// Destroys the task.
    /// 销毁任务。
    pub(crate) destroy: unsafe fn(*const ()),

    /// Runs the task.
    /// 执行任务。
    pub(crate) run: unsafe fn(*const ()) -> bool,

    /// Creates a new waker associated with the task.
    /// 新创建一个与任务关联的唤醒器。(唤醒器又引用了任务)
    pub(crate) clone_waker: unsafe fn(ptr: *const ()) -> RawWaker,

    /// The memory layout of the task. This information enables
    /// debuggers to decode raw task memory blobs. Do not remove
    /// the field, even if it appears to be unused.
    /// 任务的内存布局。使得调试器可以解码任务内存块。
    #[allow(unused)]
    pub(crate) layout_info: &'static TaskLayout,
}

/// Memory layout of a task.
/// 任务内存布局形式。包括总大小和各字段偏移量。
///
/// This struct contains the following information:
///
/// 1. How to allocate and deallocate the task.
/// 2. How to access the fields inside the task.
/// 
/// 包含的信息
/// - 1.如何分配和回收任务。
/// - 2.如何访问任务中的字段。
#[derive(Clone, Copy)]
pub(crate) struct TaskLayout {
    /// Memory layout of the whole task.
    /// 整个任务的布局。
    pub(crate) layout: StdLayout,

    /// Offset into the task at which the schedule function is stored.
    /// 调度函数的位置偏移量。
    pub(crate) offset_s: usize,

    /// Offset into the task at which the future is stored.
    /// Future对象的位置偏移量。
    pub(crate) offset_f: usize,

    /// Offset into the task at which the output is stored.
    /// Future输出的位置偏移量。
    pub(crate) offset_r: usize,
}

/// Raw pointers to the fields inside a task.
/// 任务的裸指针形式。包含各字段的裸指针。
pub(crate) struct RawTask<F, T, S, M> {
    /// The task header.
    pub(crate) header: *const Header<M>,

    /// The schedule function.
    pub(crate) schedule: *const S,

    /// The future.
    pub(crate) future: *mut F,

    /// The output of the future.
    pub(crate) output: *mut Result<T, Panic>,
}

impl<F, T, S, M> Copy for RawTask<F, T, S, M> {}

impl<F, T, S, M> Clone for RawTask<F, T, S, M> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<F, T, S, M> RawTask<F, T, S, M> {
    /// 布局常量，布局依赖于类型，因此布局可在编译时确定。
    const TASK_LAYOUT: TaskLayout = Self::eval_task_layout();

    /// Computes the memory layout for a task.
    /// 计算一个任务的内存布局，计算方法为计算各字段布局然后计算偏移量。
    #[inline]
    const fn eval_task_layout() -> TaskLayout {
        // Compute the layouts for `Header`, `S`, `F`, and `T`.
        // 计算各个字段的布局：Header, Schedule, Future, Result<T,Panic>。
        let layout_header = Layout::new::<Header<M>>();
        let layout_s = Layout::new::<S>();
        let layout_f = Layout::new::<F>();
        let layout_r = Layout::new::<Result<T, Panic>>();

        // Compute the layout for `union { F, T }`.
        // Future销毁后才得到Result，因此可以复用同一块内存，取两者最大尺寸和最大对其量。
        let size_union = max(layout_f.size(), layout_r.size());
        let align_union = max(layout_f.align(), layout_r.align());
        let layout_union = Layout::from_size_align(size_union, align_union);

        // Compute the layout for `Header` followed `S` and `union { F, T }`.
        let layout = layout_header;
        let (layout, offset_s) = leap_unwrap!(layout.extend(layout_s));
        let (layout, offset_union) = leap_unwrap!(layout.extend(layout_union));
        let offset_f = offset_union;
        let offset_r = offset_union;

        TaskLayout {
            layout: unsafe { layout.into_std() },
            offset_s,
            offset_f,
            offset_r,
        }
    }
}

impl<F, T, S, M> RawTask<F, T, S, M>
where
    F: Future<Output = T>,
    S: Schedule<M>,
{
    const RAW_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        Self::clone_waker,
        Self::wake,
        Self::wake_by_ref,
        Self::drop_waker,
    );

    /// Allocates a task with the given `future` and `schedule` function.
    /// 分配任务内存。同时对内中各任务字段执行写入：Header,Schedule,Future。
    ///
    /// It is assumed that initially only the `Runnable` and the `Task` exist.
    /// 假定初始只存在Runnable和Task。
    pub(crate) fn allocate<'a, Gen: FnOnce(&'a M) -> F>(
        future: Gen,
        schedule: S,
        builder: crate::Builder<M>,
    ) -> NonNull<()>
    where
        F: 'a,
        M: 'a,
    {
        // Compute the layout of the task for allocation. Abort if the computation fails.
        // 计算分配所用的任务布局。计算失败则终止。
        //
        // n.b. notgull: task_layout now automatically aborts instead of panicking
        let task_layout = Self::task_layout();

        unsafe {
            // Allocate enough space for the entire task.
            // 开始分配整个内存块。分配失败则终止程序。
            let ptr = match NonNull::new(alloc::alloc::alloc(task_layout.layout) as *mut ()) {
                None => abort(),
                Some(p) => p,
            };

            // 构造RawTask，即计算内存块中每个字段的指针。
            let raw = Self::from_ptr(ptr.as_ptr());

            let crate::Builder {
                metadata,
                #[cfg(feature = "std")]
                propagate_panic,
            } = builder;

            // Write the header as the first field of the task.
            // 创建默认的任务头Header并写入任务内存块。
            (raw.header as *mut Header<M>).write(Header {
                state: AtomicUsize::new(SCHEDULED | TASK | REFERENCE),
                awaiter: UnsafeCell::new(None),
                vtable: &TaskVTable {
                    schedule: Self::schedule,
                    drop_future: Self::drop_future,
                    get_output: Self::get_output,
                    drop_ref: Self::drop_ref,
                    destroy: Self::destroy,
                    run: Self::run,
                    clone_waker: Self::clone_waker,
                    layout_info: &Self::TASK_LAYOUT,
                },
                metadata,
                #[cfg(feature = "std")]
                propagate_panic,
            });

            // Write the schedule function as the third field of the task.
            // 调度函数写入内存块。
            (raw.schedule as *mut S).write(schedule);

            // Generate the future, now that the metadata has been pinned in place.
            // 利用Future生成函数和任务元数据生成Future实例。
            let future = abort_on_panic(|| future(&(*raw.header).metadata));

            // Write the future as the fourth field of the task.
            // 将Future实例写入内存块。
            raw.future.write(future);

            // 返回内存块指针。
            ptr
        }
    }

    /// Creates a `RawTask` from a raw task pointer.
    /// 直接从任务指针创建`RawTask`实例。
    #[inline]
    pub(crate) fn from_ptr(ptr: *const ()) -> Self {
        let task_layout = Self::task_layout();
        let p = ptr as *const u8;

        unsafe {
            Self {
                header: p as *const Header<M>,
                schedule: p.add(task_layout.offset_s) as *const S,
                future: p.add(task_layout.offset_f) as *mut F,
                output: p.add(task_layout.offset_r) as *mut Result<T, Panic>,
            }
        }
    }

    /// Returns the layout of the task.
    /// 返回任务的内存块布局。
    #[inline]
    fn task_layout() -> TaskLayout {
        Self::TASK_LAYOUT
    }
    /// Wakes a waker.
    /// 唤醒器四函数之一：wake。
    /// 主要任务：
    /// 1.利用调度函数调度一次任务。
    /// 2.调度完成后修改任务的状态。
    unsafe fn wake(ptr: *const ()) {
        // This is just an optimization. If the schedule function has captured variables, then
        // we'll do less reference counting if we wake the waker by reference and then drop it.
        if mem::size_of::<S>() > 0 {
            Self::wake_by_ref(ptr);
            Self::drop_waker(ptr);
            return;
        }

        // 恢复RawTask实例。
        let raw = Self::from_ptr(ptr);

        // 获取任务状态。
        let mut state = (*raw.header).state.load(Ordering::Acquire);

        loop {
            // If the task is completed or closed, it can't be woken up.
            // 如果任务已完成或已关闭,则唤醒失败,销毁唤醒器。
            if state & (COMPLETED | CLOSED) != 0 {
                // Drop the waker.
                Self::drop_waker(ptr);
                break;
            }

            // If the task is already scheduled, we just need to synchronize with the thread that
            // will run the task by "publishing" our current view of the memory.
            // 如果任务已经被调度，只需要通过发布当前内存视图，与即将执行任务的线程同步。
            // 如果任务还未被调度，则将其状态修改为已调度，
            if state & SCHEDULED != 0 {
                // Update the state without actually modifying it.
                match (*raw.header).state.compare_exchange_weak(
                    state,
                    state,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // Drop the waker.
                        Self::drop_waker(ptr);
                        break;
                    }
                    Err(s) => state = s,
                }
            } else {
                // Mark the task as scheduled.
                match (*raw.header).state.compare_exchange_weak(
                    state,
                    state | SCHEDULED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // If the task is not yet scheduled and isn't currently running, now is the
                        // time to schedule it.
                        if state & RUNNING == 0 {
                            // Schedule the task.
                            Self::schedule(ptr, ScheduleInfo::new(false));
                        } else {
                            // Drop the waker.
                            Self::drop_waker(ptr);
                        }

                        break;
                    }
                    Err(s) => state = s,
                }
            }
        }
    }

    /// Wakes a waker by reference.
    /// 唤醒器四函数之二：wake_by_ref
    unsafe fn wake_by_ref(ptr: *const ()) {
        let raw = Self::from_ptr(ptr);

        let mut state = (*raw.header).state.load(Ordering::Acquire);

        loop {
            // If the task is completed or closed, it can't be woken up.
            if state & (COMPLETED | CLOSED) != 0 {
                break;
            }

            // If the task is already scheduled, we just need to synchronize with the thread that
            // will run the task by "publishing" our current view of the memory.
            if state & SCHEDULED != 0 {
                // Update the state without actually modifying it.
                match (*raw.header).state.compare_exchange_weak(
                    state,
                    state,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(s) => state = s,
                }
            } else {
                // If the task is not running, we can schedule right away.
                let new = if state & RUNNING == 0 {
                    (state | SCHEDULED) + REFERENCE
                } else {
                    state | SCHEDULED
                };

                // Mark the task as scheduled.
                match (*raw.header).state.compare_exchange_weak(
                    state,
                    new,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // If the task is not running, now is the time to schedule.
                        if state & RUNNING == 0 {
                            // If the reference count overflowed, abort.
                            if state > isize::MAX as usize {
                                abort();
                            }

                            // Schedule the task. There is no need to call `Self::schedule(ptr)`
                            // because the schedule function cannot be destroyed while the waker is
                            // still alive.
                            let task = Runnable::from_raw(NonNull::new_unchecked(ptr as *mut ()));
                            (*raw.schedule).schedule(task, ScheduleInfo::new(false));
                        }

                        break;
                    }
                    Err(s) => state = s,
                }
            }
        }
    }

    /// Clones a waker.
    /// 唤醒器四函数之三：clone
    /// 逻辑：waker的数据部分是RawTask，因此RawTask的引用计数加一。
    unsafe fn clone_waker(ptr: *const ()) -> RawWaker {
        let raw = Self::from_ptr(ptr);

        // Increment the reference count. With any kind of reference-counted data structure,
        // relaxed ordering is appropriate when incrementing the counter.
        // 任务数据引用计数加一。
        let state = (*raw.header).state.fetch_add(REFERENCE, Ordering::Relaxed);

        // If the reference count overflowed, abort.
        if state > isize::MAX as usize {
            abort();
        }

        RawWaker::new(ptr, &Self::RAW_WAKER_VTABLE)
    }

    /// Drops a waker.
    /// 唤醒器四函数之四：drop
    ///
    /// This function will decrement the reference count. If it drops down to zero, the associated
    /// `Task` has been dropped too, and the task has not been completed, then it will get
    /// scheduled one more time so that its future gets dropped by the executor.
    /// 唤醒器引用了RawTask，因此遗弃时需要将RawTask引用计数减一，
    /// 如果减到了0对应的Task也已经被遗弃且任务仍未完成，则重新调度一次任务，让执行器遗弃它的Future。
    #[inline]
    unsafe fn drop_waker(ptr: *const ()) {
        // 还原RawTask。
        let raw = Self::from_ptr(ptr);

        // Decrement the reference count.
        // RawTask引用计数减一。
        // 标记位使用`state：AtomicUsize`的最低一个字节，引用计数使用state的其余高位字节，因此引用计数每变化1个，对应state变化2^8=256。
        let new = (*raw.header).state.fetch_sub(REFERENCE, Ordering::AcqRel) - REFERENCE;

        // If this was the last reference to the task and the `Task` has been dropped too,
        // then we need to decide how to destroy the task.
        // 如果引用计数归0了且Task已被遗弃，则我们需要决定如何销毁任务。
        // 如果任务没有完成或者关闭，则重新调度一次任务，由执行器销毁Future。
        // 如果任务已完成或者关闭，则直接销毁RawTask。
        if new & !(REFERENCE - 1) == 0 && new & TASK == 0 {
            if new & (COMPLETED | CLOSED) == 0 {
                // If the task was not completed nor closed, close it and schedule one more time so
                // that its future gets dropped by the executor.
                (*raw.header)
                    .state
                    .store(SCHEDULED | CLOSED | REFERENCE, Ordering::Release);
                Self::schedule(ptr, ScheduleInfo::new(false));
            } else {
                // Otherwise, destroy the task right away.
                Self::destroy(ptr);
            }
        }
    }

    /// Drops a task reference (`Runnable` or `Waker`).
    /// 引用计数减一。(销毁Runnable或Waker时)
    ///
    /// This function will decrement the reference count. If it drops down to zero and the
    /// associated `Task` handle has been dropped too, then the task gets destroyed.
    /// 如果减到0且关联的Task句柄已经遗弃则销毁任务。
    #[inline]
    unsafe fn drop_ref(ptr: *const ()) {
        let raw = Self::from_ptr(ptr);

        // Decrement the reference count.
        let new = (*raw.header).state.fetch_sub(REFERENCE, Ordering::AcqRel) - REFERENCE;

        // If this was the last reference to the task and the `Task` has been dropped too,
        // then destroy the task.
        if new & !(REFERENCE - 1) == 0 && new & TASK == 0 {
            Self::destroy(ptr);
        }
    }

    /// Schedules a task for running.
    /// 调度一个任务去执行。
    ///
    /// This function doesn't modify the state of the task. It only passes the task reference to
    /// its schedule function.
    /// 此函数不修改任务状态，只将任务引用传给调度函数。
    unsafe fn schedule(ptr: *const (), info: ScheduleInfo) {
        let raw = Self::from_ptr(ptr);

        // If the schedule function has captured variables, create a temporary waker that prevents
        // the task from getting deallocated while the function is being invoked.
        let _waker;
        if mem::size_of::<S>() > 0 {
            _waker = Waker::from_raw(Self::clone_waker(ptr));
        }

        let task = Runnable::from_raw(NonNull::new_unchecked(ptr as *mut ()));
        (*raw.schedule).schedule(task, info);
    }

    /// Drops the future inside a task.
    /// 遗弃任务中的Future。
    #[inline]
    unsafe fn drop_future(ptr: *const ()) {
        let raw = Self::from_ptr(ptr);

        // We need a safeguard against panics because the destructor can panic.
        abort_on_panic(|| {
            raw.future.drop_in_place();
        })
    }

    /// Returns a pointer to the output inside a task.
    /// 获取任务中的Future返回值的指针。
    unsafe fn get_output(ptr: *const ()) -> *const () {
        let raw = Self::from_ptr(ptr);
        raw.output as *const ()
    }

    /// Cleans up task's resources and deallocates it.
    /// 
    /// The schedule function will be dropped, and the task will then get deallocated.
    /// The task must be closed before this function is called.
    /// 
    /// 清理并回收任务资源。
    /// 
    /// - 遗弃调度函数值
    /// - 遗弃任务头Header
    /// - 回收RawTask内存。
    #[inline]
    unsafe fn destroy(ptr: *const ()) {
        let raw = Self::from_ptr(ptr);
        let task_layout = Self::task_layout();

        // We need a safeguard against panics because destructors can panic.
        abort_on_panic(|| {
            // Drop the header along with the metadata.
            (raw.header as *mut Header<M>).drop_in_place();

            // Drop the schedule function.
            (raw.schedule as *mut S).drop_in_place();
        });

        // Finally, deallocate the memory reserved by the task.
        alloc::alloc::dealloc(ptr as *mut u8, task_layout.layout);
    }

    /// Runs a task.
    ///
    /// If polling its future panics, the task will be closed and the panic will be propagated into
    /// the caller.
    /// 
    /// 运行一个任务。
    /// 
    /// 轮询任务的Future时如果产生恐慌，任务会被关闭并将恐慌传递给调用者。
    unsafe fn run(ptr: *const ()) -> bool {
        // 复原任务实例
        let raw = Self::from_ptr(ptr);

        // Create a context from the raw task pointer and the vtable inside the its header.
        // 创建唤醒器，创建轮询上下文。
        let waker = ManuallyDrop::new(Waker::from_raw(RawWaker::new(ptr, &Self::RAW_WAKER_VTABLE)));
        let cx = &mut Context::from_waker(&waker);

        // 加载任务状态值。
        let mut state = (*raw.header).state.load(Ordering::Acquire);

        // Update the task's state before polling its future.
        // ① 轮询Future之前初始化任务状态。
        // - 如果任务已关闭，直接清理并返回.
        // - 如果任务未关闭，标记为未调度、运行中。
        loop {
            // If the task has already been closed, drop the task reference and return.
            if state & CLOSED != 0 {
                // Drop the future.
                // 遗弃Future。
                Self::drop_future(ptr);

                // Mark the task as unscheduled.
                // 标记为未调度。
                let state = (*raw.header).state.fetch_and(!SCHEDULED, Ordering::AcqRel);

                // Take the awaiter out.
                // 取出任务等待者。
                let mut awaiter = None;
                if state & AWAITER != 0 {
                    awaiter = (*raw.header).take(None);
                }

                // Drop the task reference.
                // 遗弃一个引用。
                Self::drop_ref(ptr);

                // Notify the awaiter that the future has been dropped.
                // 唤醒Task::await()。
                if let Some(w) = awaiter {
                    abort_on_panic(|| w.wake());
                }
                return false;
            }

            // Mark the task as unscheduled and running.
            match (*raw.header).state.compare_exchange_weak(
                state,
                (state & !SCHEDULED) | RUNNING,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Update the state because we're continuing with polling the future.
                    state = (state & !SCHEDULED) | RUNNING;
                    break;
                }
                Err(s) => state = s,
            }
        }

        // Poll the inner future, but surround it with a guard that closes the task in case polling
        // panics.
        // If available, we should also try to catch the panic so that it is propagated correctly.
        // 轮询内部的Future，设置一个资源守卫用于在Future产生异常后能够正常清理。
        let guard = Guard(raw);

        // Panic propagation is not available for no_std.
        // 在no_std中无恐慌传播逻辑。
        #[cfg(not(feature = "std"))]
        let poll = <F as Future>::poll(Pin::new_unchecked(&mut *raw.future), cx).map(Ok);

        // 在std中启用恐慌传播逻辑。
        #[cfg(feature = "std")]
        let poll = {
            // Check if we should propagate panics.
            if (*raw.header).propagate_panic {
                // Use catch_unwind to catch the panic.
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    <F as Future>::poll(Pin::new_unchecked(&mut *raw.future), cx)
                })) {
                    Ok(Poll::Ready(v)) => Poll::Ready(Ok(v)),
                    Ok(Poll::Pending) => Poll::Pending,
                    Err(e) => Poll::Ready(Err(e)),
                }
            } else {
                <F as Future>::poll(Pin::new_unchecked(&mut *raw.future), cx).map(Ok)
            }
        };

        // 走到这一步说明没有恐慌，遗忘守卫，避免二次释放。
        mem::forget(guard);

        // 处理轮询结果
        // - Poll::Ready：复用任务中Future的内存位置，写入Future执行结果，并唤醒Task::await()。
        // - Poll::Pending：
        match poll {
            Poll::Ready(out) => {
                // Replace the future with its output.
                Self::drop_future(ptr);
                raw.output.write(out);

                // The task is now completed.
                loop {
                    // If the `Task` is dropped, we'll need to close it and drop the output.
                    // 如果没有`Task`句柄存活，标记为完成、关闭。
                    // 如果有`Task`句柄存活，标记为完成。
                    let new = if state & TASK == 0 {
                        (state & !RUNNING & !SCHEDULED) | COMPLETED | CLOSED
                    } else {
                        (state & !RUNNING & !SCHEDULED) | COMPLETED
                    };

                    // Mark the task as not running and completed.
                    // 乐观修改任务状态
                    // - 如果有的话，唤醒Task::await。
                    // - 如果没有，直接销毁任务输出，
                    match (*raw.header).state.compare_exchange_weak(
                        state,
                        new,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            // If the `Task` is dropped or if the task was closed while running,
                            // now it's time to drop the output.
                            // 如果没有`Task`句柄存活或任务在运行时已关闭，则需要丢弃任务输出。
                            if state & TASK == 0 || state & CLOSED != 0 {
                                // Drop the output.
                                abort_on_panic(|| raw.output.drop_in_place());
                            }

                            // Take the awaiter out.
                            // 如果有`Task`句柄存活，则需要从任务中取出它的唤醒器，将其唤醒。
                            let mut awaiter = None;
                            if state & AWAITER != 0 {
                                awaiter = (*raw.header).take(None);
                            }

                            // Drop the task reference.
                            // 唤醒器消耗后，任务的引用计数需要减一。
                            Self::drop_ref(ptr);

                            // Notify the awaiter that the future has been dropped.
                            // 执行唤醒`Task::await`
                            if let Some(w) = awaiter {
                                abort_on_panic(|| w.wake());
                            }
                            break;
                        }
                        Err(s) => state = s,
                    }
                }
            }
            Poll::Pending => {
                let mut future_dropped = false;

                // The task is still not completed.
                // 任务仍未完成。
                loop {
                    // If the task was closed while running, we'll need to unschedule in case it
                    // was woken up and then destroy it.
                    // 如果任务在运行时关闭了，标记为未调度、未运行。
                    // 否则，标记为未运行。
                    let new = if state & CLOSED != 0 {
                        state & !RUNNING & !SCHEDULED
                    } else {
                        state & !RUNNING
                    };

                    // 如果任务关闭了，销毁Future。
                    if state & CLOSED != 0 && !future_dropped {
                        // The thread that closed the task didn't drop the future because it was
                        // running so now it's our responsibility to do so.
                        Self::drop_future(ptr);
                        future_dropped = true;
                    }

                    // Mark the task as not running.
                    // 将任务标记为未运行。
                    match (*raw.header).state.compare_exchange_weak(
                        state,
                        new,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(state) => {
                            // If the task was closed while running, we need to notify the awaiter.
                            // If the task was woken up while running, we need to schedule it.
                            // Otherwise, we just drop the task reference.
                            // 如果任务是关闭状态，需要唤醒awaiter。
                            // 如果任务是调度状态，则对其进行调度。
                            // 否则，直接丢弃一个任务引用。???
                            if state & CLOSED != 0 {
                                // Take the awaiter out.
                                let mut awaiter = None;
                                if state & AWAITER != 0 {
                                    awaiter = (*raw.header).take(None);
                                }

                                // Drop the task reference.
                                Self::drop_ref(ptr);

                                // Notify the awaiter that the future has been dropped.
                                if let Some(w) = awaiter {
                                    abort_on_panic(|| w.wake());
                                }
                            } else if state & SCHEDULED != 0 {
                                // The thread that woke the task up didn't reschedule it because
                                // it was running so now it's our responsibility to do so.
                                Self::schedule(ptr, ScheduleInfo::new(true));
                                return true;
                            } else {
                                // Drop the task reference.
                                Self::drop_ref(ptr);
                            }
                            break;
                        }
                        Err(s) => state = s,
                    }
                }
            }
        }

        return false;

        /// A guard that closes the task if polling its future panics.
        /// 资源守卫，当轮询Future产生恐慌时，可以正常关闭任务。
        struct Guard<F, T, S, M>(RawTask<F, T, S, M>)
        where
            F: Future<Output = T>,
            S: Schedule<M>;

        impl<F, T, S, M> Drop for Guard<F, T, S, M>
        where
            F: Future<Output = T>,
            S: Schedule<M>,
        {
            fn drop(&mut self) {
                let raw = self.0;
                let ptr = raw.header as *const ();

                unsafe {
                    let mut state = (*raw.header).state.load(Ordering::Acquire);

                    loop {
                        // If the task was closed while running, then unschedule it, drop its
                        // future, and drop the task reference.
                        if state & CLOSED != 0 {
                            // The thread that closed the task didn't drop the future because it
                            // was running so now it's our responsibility to do so.
                            RawTask::<F, T, S, M>::drop_future(ptr);

                            // Mark the task as not running and not scheduled.
                            (*raw.header)
                                .state
                                .fetch_and(!RUNNING & !SCHEDULED, Ordering::AcqRel);

                            // Take the awaiter out.
                            let mut awaiter = None;
                            if state & AWAITER != 0 {
                                awaiter = (*raw.header).take(None);
                            }

                            // Drop the task reference.
                            RawTask::<F, T, S, M>::drop_ref(ptr);

                            // Notify the awaiter that the future has been dropped.
                            if let Some(w) = awaiter {
                                abort_on_panic(|| w.wake());
                            }
                            break;
                        }

                        // Mark the task as not running, not scheduled, and closed.
                        match (*raw.header).state.compare_exchange_weak(
                            state,
                            (state & !RUNNING & !SCHEDULED) | CLOSED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(state) => {
                                // Drop the future because the task is now closed.
                                RawTask::<F, T, S, M>::drop_future(ptr);

                                // Take the awaiter out.
                                let mut awaiter = None;
                                if state & AWAITER != 0 {
                                    awaiter = (*raw.header).take(None);
                                }

                                // Drop the task reference.
                                RawTask::<F, T, S, M>::drop_ref(ptr);

                                // Notify the awaiter that the future has been dropped.
                                if let Some(w) = awaiter {
                                    abort_on_panic(|| w.wake());
                                }
                                break;
                            }
                            Err(s) => state = s,
                        }
                    }
                }
            }
        }
    }
}
