package burst

type task struct {
	in  interface{}
	out chan interface{}
}

type Manager struct {
	batch      chan *task
	QueueSize  int
	BatchSize  int
	NumWorkers int
	F          func(in []interface{}) (out []interface{})
}

func (m *Manager) Start() {
	if m.F == nil {
		panic("F is nil")
	}
	if m.QueueSize == 0 {
		m.QueueSize = 1024
	}
	if m.NumWorkers == 0 {
		m.NumWorkers = 1
	}
	if m.BatchSize == 0 {
		m.BatchSize = 16
	}

	m.batch = make(chan *task, m.QueueSize)
	for i := 0; i < m.NumWorkers; i++ {
		go func() {
			tasks := []*task{}
			blocking := false

			for {
				if blocking {
					t := <-m.batch
					tasks = append(tasks, t)
				} else {
					for exit := false; !exit; {
						select {
						case t := <-m.batch:
							tasks = append(tasks, t)
							if len(tasks) >= m.BatchSize {
								exit = true
							}
						default:
							exit = true
						}
					}
				}

				if len(tasks) == 0 {
					blocking = true
					continue
				}

				blocking = false

				keys := make([]interface{}, len(tasks))
				for i := range tasks {
					keys[i] = tasks[i].in
				}

				out := m.F(keys)
				for i, t := range tasks {
					t.out <- out[i]
				}
				tasks = tasks[:0]
			}
		}()
	}
}

func (m *Manager) Do(in interface{}) (interface{}, error) {
	task := &task{
		in:  in,
		out: make(chan interface{}, 1),
	}
	m.batch <- task
	v := <-task.out
	if e, ok := v.(error); ok {
		return nil, e
	}
	return v, nil
}
