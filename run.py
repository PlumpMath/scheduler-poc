import uuid
import time



def coroutine(fn):
    def wrapper(*args, **kwargs):
        result = fn(*args, **kwargs)
        next(result)
        return result
    return wrapper


class Task(object):
    def __init__(self, name):
        self.name = name
        self.id = uuid.uuid4()
        self.dependencies = []
        self.runner = None

    def add_dependency(self, task):
        self.dependencies.append(task)
    
    def handle_finished(self, _id):
        for task in self.dependencies:
            if task.id == _id:
                print('Executing task: {} due to finished task: {}'.format(self.name, _id))
                self.runner.close_task(_id)

    @coroutine
    def start(self, runner):
        self.runner = runner
        print('Task: {} started'.format(self.id))
        while True:
            try:
                event = (yield)
            except StopIteration:
                print('Task: {} exited'.format(self.id))

            finished = event.get('finished')
            if finished:
                self.handle_finished(finished) 

            print('Task: {} received event: {}'.format(self.id, event))


class Runner(object):
    def __init__(self, tasks=None):
        self.tasks = tasks or {}

    def add_task(self, task):
        if task not in self.tasks:
            self.tasks[task.id] = {
                'instance': task,
                'coroutine': task.start(self)
            }

    def close_task(self, _id):
        task = self.tasks.get(_id)
        if task:
            task['coroutine'].close()
            del(self.tasks[_id])

    def dispatch(self, event):
        for _id, task in self.tasks.items():
            task['coroutine'].send(event)


if __name__ == '__main__':

    runner = Runner()

    root_task = Task('Root')
    child_one = Task('One')
    child_two = Task('Two')  
    child_three = Task('Three')  

    child_one.add_dependency(root_task)
    child_two.add_dependency(root_task)
    child_three.add_dependency(child_two)

    runner.add_task(root_task)
    runner.add_task(child_one)
    runner.add_task(child_two)
    runner.add_task(child_three)

    while True:
        runner.dispatch({'finished': root_task.id})
        time.sleep(5)
        runner.dispatch({'finished': child_two.id})
