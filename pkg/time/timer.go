package time

import (
	"sync"
	itime "time"

	log "github.com/golang/glog"
)

const (
	timeFormat       = "2006-01-02 15:04:05"     // 时间格式
	infiniteDuration = itime.Duration(1<<63 - 1) // 无限长的持续时间
)

// TimerTask 每个定时任务抽象出的基本结构（是个链表 Next）
type TimerTask struct {
	Key      string     // 唯一标识符
	Expire   itime.Time // 到期时间
	Callback func()     // 到期回调函数

	Index int        // 任务在堆中的索引
	Next  *TimerTask // 下一个空闲的任务节点
}

// Delay 计算任务的延迟时间 time.Until
func (task *TimerTask) Delay() itime.Duration {
	return itime.Until(task.Expire)
}

// ExpireString 返回任务到期时间的格式化字符串
func (task *TimerTask) ExpireString() string {
	return task.Expire.Format(timeFormat)
}

// Timer 定时器结构
type Timer struct {
	mutex    sync.Mutex // 互斥锁
	freeList *TimerTask // 空闲任务链表 TimerTask -> TimerTask -> ... -> nil

	// 使用小顶堆维护的任务列表，这里实现了一个堆排序的算法，每次加入数据都会调整堆
	// 小顶堆（即最小堆），则最久的任务是在堆顶（优先级最高）
	taskHeap []*TimerTask

	signalTimer *itime.Timer // 定时器信号
	capacity    int          // TimerTask 任务容量个数
}

// NewTimer 创建一个新的定时器
func NewTimer(capacity int) *Timer {
	t := &Timer{}
	t.init(capacity)
	return t
}

// Init 初始化定时器
func (t *Timer) Init(capacity int) {
	t.init(capacity)
}

// init 内部初始化函数
func (t *Timer) init(capacity int) {
	t.signalTimer = itime.NewTimer(infiniteDuration)
	t.taskHeap = make([]*TimerTask, 0, capacity)
	t.capacity = capacity
	t.expandFreeList() // 初始化空闲任务链表
	go t.run()         // 开启定时器循环
}

// expandFreeList 扩展空闲任务链表
func (t *Timer) expandFreeList() {
	taskSlice := make([]TimerTask, t.capacity)
	t.freeList = &taskSlice[0]
	currentTask := t.freeList
	for i := 1; i < t.capacity; i++ {
		currentTask.Next = &taskSlice[i]
		currentTask = currentTask.Next
	}
	currentTask.Next = nil // 链表结束
}

// getFreeTask 获取一个空闲任务节点
func (t *Timer) getFreeTask() *TimerTask {
	if t.freeList == nil {
		t.expandFreeList() // 如果空闲链表为空，扩展链表
	}
	task := t.freeList
	t.freeList = task.Next
	return task
}

// releaseTask 释放一个任务节点，放回空闲链表
func (t *Timer) releaseTask(task *TimerTask) {
	task.Callback = nil
	task.Next = t.freeList
	t.freeList = task
}

// Add 添加一个新的定时任务
func (t *Timer) Add(delay itime.Duration, callback func()) *TimerTask {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	task := t.getFreeTask()
	task.Expire = itime.Now().Add(delay)
	task.Callback = callback
	t.push(task) // 将任务加入堆中
	return task
}

// Remove 删除一个定时任务
func (t *Timer) Remove(task *TimerTask) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.remove(task)
	t.releaseTask(task)
}

// push 将任务添加到最小堆中
func (t *Timer) push(task *TimerTask) {
	task.Index = len(t.taskHeap)
	t.taskHeap = append(t.taskHeap, task)
	t.heapifyUp(task.Index)

	// 如果插入的是堆顶任务，重置定时器
	if task.Index == 0 {
		delay := task.Delay()
		t.signalTimer.Reset(delay)
		if Debug {
			log.Infof("定时器: 添加并重置延迟 %d 毫秒", int64(delay)/int64(itime.Millisecond))
		}
	}

	if Debug {
		log.Infof("定时器: 添加任务 key: %s, 到期时间: %s, 索引: %d", task.Key, task.ExpireString(), task.Index)
	}
}

// remove 从堆中删除一个任务，注意：从堆中删除一个元素时，并不仅仅是删除第一个元素，还要保持堆的性质
func (t *Timer) remove(task *TimerTask) {
	index := task.Index
	lastIndex := len(t.taskHeap) - 1

	if index < 0 || index > lastIndex || t.taskHeap[index] != task {
		if Debug {
			log.Infof("定时器删除失败: 索引 %d, 最后索引: %d, 任务地址: %p", index, lastIndex, task)
		}
		return
	}

	if index != lastIndex {
		t.swap(index, lastIndex)
		t.heapifyDown(index, lastIndex)
		t.heapifyUp(index)
	}
	t.taskHeap = t.taskHeap[:lastIndex] // 移除最后一个元素

	if Debug {
		log.Infof("定时器: 删除任务 key: %s, 到期时间: %s, 索引: %d", task.Key, task.ExpireString(), task.Index)
	}
}

// Update 更新 timerTask 的过期时间
func (t *Timer) Update(td *TimerTask, expire itime.Duration) {
	t.mutex.Lock()
	// 先从堆中移除
	t.remove(td)
	// 更新新的过期时间
	td.Expire = itime.Now().Add(expire)
	// 重新放入堆中
	t.push(td)
	t.mutex.Unlock()
}

// run 启动定时器循环
func (t *Timer) run() {
	for {
		t.processExpiredTasks()
		<-t.signalTimer.C
	}
}

// processExpiredTasks 处理到期的任务
func (t *Timer) processExpiredTasks() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	for {
		if len(t.taskHeap) == 0 {
			t.signalTimer.Reset(infiniteDuration)
			if Debug {
				log.Info("定时器: 没有任务")
			}
			break
		}

		task := t.taskHeap[0]
		delay := task.Delay()

		if delay > 0 {
			t.signalTimer.Reset(delay)
			break
		}

		callback := task.Callback

		// 从 taskHeap 中移除 task，并且重新调整堆
		t.remove(task)

		if callback == nil {
			log.Warning("到期任务没有回调函数")
		} else {
			if Debug {
				log.Infof("定时器任务 key: %s, 到期时间: %s, 索引: %d 已到期，执行回调", task.Key, task.ExpireString(), task.Index)
			}
			go callback()
		}
	}
}

// heapifyUp 上浮调整堆
func (t *Timer) heapifyUp(index int) {
	for {
		parent := (index - 1) / 2
		if index <= 0 || !t.less(index, parent) {
			break
		}
		t.swap(index, parent)
		index = parent
	}
}

// heapifyDown 下沉调整堆
func (t *Timer) heapifyDown(index, n int) {
	for {
		leftChild := 2*index + 1
		if leftChild >= n || leftChild < 0 {
			break
		}
		smallest := leftChild
		if rightChild := leftChild + 1; rightChild < n && !t.less(leftChild, rightChild) {
			smallest = rightChild
		}
		if !t.less(smallest, index) {
			break
		}
		t.swap(index, smallest)
		index = smallest
	}
}

// less 比较两个任务的到期时间，决定堆的顺序
func (t *Timer) less(i, j int) bool {
	return t.taskHeap[i].Expire.Before(t.taskHeap[j].Expire)
}

// swap 交换两个任务在堆中的位置
func (t *Timer) swap(i, j int) {
	t.taskHeap[i], t.taskHeap[j] = t.taskHeap[j], t.taskHeap[i]
	t.taskHeap[i].Index = i
	t.taskHeap[j].Index = j
}
