//
//  lowlevel.c
//  await-test
//
//  Created by Alastair Houghton on 10/06/2014.
//  Copyright (c) 2014 Coriolis Systems. All rights reserved.
//

#include <CoreFoundation/CoreFoundation.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <pthread.h>
#include <assert.h>

#include "include/lowlevel.h"

#define ASYNC_STACK_SIZE 16384
#define ASYNC_POOL_SIZE  (32 * ASYNC_STACK_SIZE)

struct async_ll_stack {
  struct async_ll_stack *next;
};

struct async_ll_arena {
  struct async_ll_arena *next;
};

struct async_ll_thread {
  pthread_mutex_t     q_mutex;
  pthread_cond_t      q_cond;
  struct async_ll_task  *ready_q;
  struct async_ll_task  *current_task;
  dispatch_queue_t    dispatch_q;
  CFRunLoopSourceRef  source;
  CFRunLoopRef        runloop;
  
  struct async_ll_arena *arenas;
  
  struct async_ll_arena *current_arena;
  uint8_t            *arena_ptr;
  uint8_t            *arena_end;
  
  struct async_ll_stack *free_stacks;
};

struct async_ll_task {
  bool                 done;
  struct async_ll_thread *owner;
  struct async_ll_task   *next_ready;
  int64_t              result;
  jmp_buf              buf;
  void                *stack_base;
  void                *arg;
  int64_t             (*entry_point)(void *arg);
  CFTypeRef           retained;
  struct async_ll_task   *caller;
  struct async_ll_task   *awaiting;
};

static int64_t call_block(void *arg) {
  int64_t (^blk)() = (int64_t (^)())arg;
  return blk();
}

static bool async_ll_run_next(void);
static bool async_ll_run_all_blocking(void);
static void async_ll_perform(void *info);
static struct async_ll_thread *async_ll_current_thread(void);
static void async_ll_switch_to(async_ll_task_t task);
static void *async_ll_alloc_stack(struct async_ll_thread *thread);
static void async_ll_release_stack(struct async_ll_thread *thread, void *base);

void enter_task(async_ll_task_t task) {
  task->result = task->entry_point (task->arg);
  task->done = true;
  if (task->awaiting)
    async_ll_switch_to (task->awaiting);
  async_ll_switch_to (task->caller);
}

int64_t call_with_stack(async_ll_task_t task,
                        void *stack_top) __attribute__((noreturn));

#if defined(__x86_64__)
__asm__ ("\
         .text\n\
         .align 4\n\
_call_with_stack:\n\
         movq  %rsi,%rsp\n\
         pushq %rbp\n\
         subq  $8,%rsp\n\
         callq _enter_task\n\
         int   $3\n\
         ");
#elif defined(__i386__)
__asm__ ("\
         .text\n\
         .align 4\n\
_call_with_stack:\n\
         movl  4(%esp),%eax\n\
         movl  8(%esp),%esp\n\
         pushl %eax\n\
         subl  $12,%esp\n\
         calll _enter_task\n\
         int   $3\n\
         ");
#elif defined(__arm__)
__asm__ ("\
         .text\n\
         .align 4\n\
_call_with_stack:\n\
         mov   sp,r1\n\
         b     _enter_task;\n\
         ");
#elif defined(__arm64__)
__asm__ ("\
         .text\n\
         .align 4\n\
_call_with_stack:\n\
         mov   sp,x1\n\
         b     _enter_task\n\
         ");
#else
#error You need to write code for your CPU
#endif

static pthread_key_t async_ll_key;
static void async_ll_teardown(void *arg);

static void
async_ll_init(void)
{
  pthread_key_create (&async_ll_key, async_ll_teardown);
}

static void
async_ll_teardown(void *arg)
{
  struct async_ll_thread *thread = (struct async_ll_thread *)arg;
  
  for (struct async_ll_arena *arena = thread->arenas, *next_arena;
       arena;
       arena = next_arena) {
    next_arena = arena->next;
    munmap (arena, ASYNC_POOL_SIZE);
  }
  
  pthread_mutex_destroy (&thread->q_mutex);
  pthread_cond_destroy (&thread->q_cond);
  
  free (thread);
}

static struct async_ll_thread *
async_ll_current_thread(void)
{
  static pthread_once_t once = PTHREAD_ONCE_INIT;
  struct async_ll_thread *thread;
  
  pthread_once(&once, async_ll_init);
  
  thread = pthread_getspecific (async_ll_key);
  
  if (!thread) {
    size_t size = sizeof(struct async_ll_thread) + sizeof(struct async_ll_task);
    thread = (struct async_ll_thread *)malloc (size);
    
    memset (thread, 0, size);
    pthread_mutex_init (&thread->q_mutex, NULL);
    pthread_cond_init (&thread->q_cond, NULL);
    thread->current_task = (struct async_ll_task *)(thread + 1);
    thread->current_task->owner = thread;
    
    pthread_setspecific (async_ll_key, thread);
  }
  
  return thread;
}

void
async_ll_schedule_in_runloop(CFRunLoopRef runLoop)
{
  struct async_ll_thread *thread = async_ll_current_thread();
  CFRunLoopSourceContext ctx;
  
  memset (&ctx, 0, sizeof(ctx));
  
  ctx.perform = async_ll_perform;
  
  thread->runloop = runLoop;
  thread->source = CFRunLoopSourceCreate (kCFAllocatorDefault, 0, &ctx);
  
  CFRunLoopAddSource (thread->runloop, thread->source, kCFRunLoopCommonModes);
}

void
async_ll_schedule_in_queue(dispatch_queue_t queue)
{
  struct async_ll_thread *thread = async_ll_current_thread();

  thread->dispatch_q = queue;
}

void
async_ll_unschedule()
{
  struct async_ll_thread *thread = async_ll_current_thread();

  thread->dispatch_q = NULL;
  
  if (thread->source) {
    CFRunLoopSourceInvalidate (thread->source);
    CFRelease (thread->source);
    thread->source = NULL;
    thread->runloop = NULL;
  }
}

static void
async_ll_switch_to(async_ll_task_t task)
{
  struct async_ll_thread *thread = async_ll_current_thread();
  thread->current_task = task;
  _longjmp (thread->current_task->buf, 1);
}

static async_ll_task_t
async_ll_call_fn_impl(void *arg,
                   int64_t (*pfn)(void *arg),
                   CFTypeRef retained)
{
  struct async_ll_thread *thread = async_ll_current_thread();
  volatile struct async_ll_task *task
  = (struct async_ll_task *)malloc (sizeof (struct async_ll_task));
  
  memset ((void *)task, 0, sizeof(struct async_ll_task));
  task->owner = (struct async_ll_thread *)thread;
  
  if (_setjmp(thread->current_task->buf))
    return (async_ll_task_t)task;
  
  task->caller = thread->current_task;
  thread->current_task = (async_ll_task_t)task;
  
  // Call with a new stack
  volatile void *base = async_ll_alloc_stack(thread);
  void *top = (char *)base + ASYNC_STACK_SIZE;
  
  task->stack_base = (void *)base;
  task->entry_point = pfn;
  task->arg = arg;
  task->retained = retained ? CFRetain(retained) : NULL;
  
  call_with_stack ((async_ll_task_t)task, top);
}


async_ll_task_t
async_ll_call_fn(void *arg,
              int64_t (*pfn)(void *arg))
{
  return async_ll_call_fn_impl (arg, pfn, NULL);
}

async_ll_task_t
async_ll_call(int64_t (^blk)(void))
{
  return async_ll_call_fn_impl (blk, call_block, blk);
}

int64_t async_ll_await(async_ll_task_t t)
{
  volatile async_ll_task_t task = t;
  volatile struct async_ll_thread *thread = async_ll_current_thread();
  int64_t result;
  
  /* Must have the same owning thread */
  assert(t->owner == thread);
  
  while (!task->done) {
    if (_setjmp (thread->current_task->buf) == 0) {
      task->awaiting = thread->current_task;
      if (!thread->current_task->caller) {
        /* Don't allow await()ing from the run loop or from a dispatch
           queue without being in asynchronous context */
        assert(!thread->runloop && !thread->dispatch_q);
        
        async_ll_run_all_blocking();
      } else {
        async_ll_switch_to (thread->current_task->caller);
      }
    } else {
      task->awaiting = NULL;
    }
  }
  
  async_ll_release_stack((struct async_ll_thread *)thread, task->stack_base);
  if (task->retained)
    CFRelease (task->retained);
  result = task->result;
  free (task);
  return result;
}

void
async_ll_suspend()
{
  volatile struct async_ll_thread *thread = async_ll_current_thread();
  if (_setjmp (thread->current_task->buf) == 0)
    async_ll_switch_to (thread->current_task->caller);
}

void
async_ll_wake(async_ll_task_t task)
{
  struct async_ll_thread *owner = task->owner;
  pthread_mutex_lock (&owner->q_mutex);
  if (!task->next_ready) {
    if (owner->ready_q) {
      task->next_ready = owner->ready_q->next_ready;
      owner->ready_q->next_ready = task;
    } else {
      task->next_ready = task;
      owner->ready_q = task;
    }
  }

  if (owner->dispatch_q) {
    dispatch_async (owner->dispatch_q, ^{
      while (async_ll_run_next());
    });
  } else if (owner->source) {
    CFRunLoopSourceSignal (owner->source);
    CFRunLoopWakeUp (owner->runloop);
  } else
    pthread_cond_signal (&owner->q_cond);
  
  pthread_mutex_unlock (&owner->q_mutex);
}

static bool
async_ll_run_next(void)
{
  volatile struct async_ll_thread *thread = async_ll_current_thread();
  volatile async_ll_task_t task = NULL;
  
  pthread_mutex_lock ((pthread_mutex_t *)&thread->q_mutex);
  if (thread->ready_q) {
    task = thread->ready_q->next_ready;
    thread->ready_q = task->next_ready;
    if (thread->ready_q == task)
      thread->ready_q = NULL;
  }
  pthread_mutex_unlock ((pthread_mutex_t *)&thread->q_mutex);
  
  if (task && _setjmp (thread->current_task->buf) == 0) {
    task->caller = thread->current_task;
    async_ll_switch_to (task);
  }
  
  return task;
}

static bool
async_ll_run_all_blocking(void)
{
  volatile struct async_ll_thread *thread = async_ll_current_thread();
  volatile async_ll_task_t task = NULL;
  volatile bool ran_one = false;
  
  pthread_mutex_lock ((pthread_mutex_t *)&thread->q_mutex);
  if (!thread->ready_q) {
    pthread_cond_wait ((pthread_cond_t *)&thread->q_cond,
                       (pthread_mutex_t *)&thread->q_mutex);
  }
  if (thread->ready_q) {
    task = thread->ready_q->next_ready;
    thread->ready_q = task->next_ready;
    if (thread->ready_q == task)
      thread->ready_q = NULL;
  }
  pthread_mutex_unlock ((pthread_mutex_t *)&thread->q_mutex);
  
  if (task && _setjmp (thread->current_task->buf) == 0) {
    ran_one = true;
    task->caller = thread->current_task;
    async_ll_switch_to (task);
  }
  
  return ran_one;
}

static void
async_ll_perform(__unused void *info)
{
  while (async_ll_run_next());
}

async_ll_task_t
async_ll_current_task(void)
{
  return async_ll_current_thread()->current_task;
}

bool
async_ll_done(async_ll_task_t task)
{
  return task->done;
}

static void *
async_ll_alloc_stack(struct async_ll_thread *thread)
{
  uint8_t *base;
  
  if (thread->free_stacks) {
    struct async_ll_stack *stack = thread->free_stacks->next;
    if (stack == thread->free_stacks)
      thread->free_stacks = NULL;
    else
      thread->free_stacks->next = stack->next;
    
    base = (uint8_t *)stack - 4096;
    
    return base;
  }
  
  if (thread->arena_ptr + ASYNC_STACK_SIZE >= thread->arena_end) {
    struct async_ll_arena *the_arena;
    
    thread->arena_ptr = mmap (NULL, ASYNC_POOL_SIZE,
                               PROT_READ|PROT_WRITE,
                               MAP_ANON|MAP_PRIVATE, -1, 0);
    thread->arena_end = thread->arena_ptr;
    
    the_arena = (struct async_ll_arena *)(thread->arena_ptr);
    the_arena->next = thread->current_arena;
    thread->current_arena = the_arena;
  }
  
  base = thread->arena_ptr;
  thread->arena_ptr += ASYNC_STACK_SIZE;
  
  /* Protect the bottom page; this is sufficient to cause stack overruns to
     crash the process, without forcing us to call mprotect() to read the
     header. */
  mprotect (base, 4096, PROT_READ);
  
  return base;
}

static void
async_ll_release_stack(struct async_ll_thread *thread, void *base)
{
  struct async_ll_stack *stack = (struct async_ll_stack *)((uint8_t *)base + 4096);
  
  if (thread->free_stacks) {
    stack->next = thread->free_stacks->next;
    thread->free_stacks->next = stack;
  } else {
    stack->next = stack;
    thread->free_stacks = stack;
  }
}
